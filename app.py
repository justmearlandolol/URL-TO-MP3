#!/usr/bin/env python3
"""
YouTube -> MP3 / MP4 downloader.

Backend: Flask + yt-dlp. Progress is REAL, not faked:
  * download phase  -> yt-dlp progress hooks (bytes downloaded / total bytes)
  * convert phase   -> ffmpeg `-progress pipe:1`, percent derived from
                       out_time_us vs the media duration known up-front
Both phases are streamed to the browser over SSE (Server-Sent Events).

Run:  python app.py            (then open http://localhost:5000)
Env:  PORT, YTDL_DOWNLOAD_DIR, YTDL_KEEP_MINUTES
"""

from __future__ import annotations

import atexit
import json
import os
import queue
import re
import shutil
import subprocess
import tempfile
import threading
import time
import unicodedata
import urllib.request
import uuid
from pathlib import Path
from urllib.parse import quote, urlparse

from flask import (
    Flask,
    Response,
    jsonify,
    render_template,
    request,
    send_from_directory,
)
import yt_dlp

BASE_DIR = Path(__file__).resolve().parent
DOWNLOAD_DIR = Path(os.environ.get("YTDL_DOWNLOAD_DIR", BASE_DIR / "downloads"))
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
KEEP_SECONDS = int(os.environ.get("YTDL_KEEP_MINUTES", "60")) * 60
PORT = int(os.environ.get("PORT", "5000"))
JOB_ID_RE = re.compile(r"^[a-f0-9]{12}$")
# Optional HTTP Basic Auth for public deployments (Render, VPS, ...).
# Only enforced when BOTH env vars are set. /healthz and /ping stay public
# so uptime monitors / ping bots do not need credentials.
AUTH_USER = os.environ.get("YTDL_USER", "").strip()
AUTH_PASS = os.environ.get("YTDL_PASS", "").strip()
# Cookies (Netscape cookies.txt) + extractor args: penangkal utama error
# "Sign in to confirm you're not a bot" yang sering kena ke IP datacenter.
COOKIEFILE = os.environ.get("YTDL_COOKIEFILE", "").strip()
EXTRACTOR_ARGS = os.environ.get("YTDL_EXTRACTOR_ARGS", "").strip()
VERSION = "1.2.0"
START_TIME = time.time()

app = Flask(__name__)
JOBS: dict[str, dict] = {}
JOBS_LOCK = threading.Lock()


@app.before_request
def basic_auth_gate():
    if not (AUTH_USER and AUTH_PASS):
        return None
    if request.path in ("/healthz", "/ping"):
        return None
    auth = request.authorization
    if auth and auth.type == "basic" and auth.username == AUTH_USER and auth.password == AUTH_PASS:
        return None
    return Response(
        "Autentikasi diperlukan.",
        401,
        {"WWW-Authenticate": 'Basic realm="YTDL" charset="UTF-8"',
         "Cache-Control": "no-store"},
    )


# --------------------------------------------------------------------------
# ffmpeg discovery (system PATH -> imageio-ffmpeg -> common locations)
# --------------------------------------------------------------------------
def find_ffmpeg() -> str | None:
    exe = shutil.which("ffmpeg")
    if exe:
        return exe
    try:
        import imageio_ffmpeg  # pip install imageio-ffmpeg

        return imageio_ffmpeg.get_ffmpeg_exe()
    except Exception:
        pass
    for cand in ("/usr/bin/ffmpeg", "/usr/local/bin/ffmpeg", "/opt/homebrew/bin/ffmpeg"):
        if os.path.isfile(cand) and os.access(cand, os.X_OK):
            return cand
    return None


FFMPEG = find_ffmpeg()


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------
def safe_name(title: str) -> str:
    t = unicodedata.normalize("NFKD", title or "").encode("ascii", "ignore").decode()
    t = re.sub(r"[^\w\s\-.()]", "", t).strip()
    t = re.sub(r"\s+", " ", t)
    return (t[:80].strip(" .") or "media")


def clean_error(exc: Exception) -> str:
    msg = str(exc).replace("ERROR:", "").strip()
    msg = re.sub(r"\x1b\[[0-9;]*m", "", msg)
    for line in msg.splitlines():
        if line.strip():
            return line.strip()[:300]
    return "Terjadi kesalahan."


def parse_extractor_args(raw: str) -> dict:
    """'youtube:player_client=web_safari|tv' -> {'youtube': {'player_client': ['web_safari','tv']}}"""
    out: dict[str, dict[str, list[str]]] = {}
    for part in (raw or "").split(","):
        part = part.strip()
        if ":" not in part or "=" not in part:
            continue
        ie, rest = part.split(":", 1)
        key, val = rest.split("=", 1)
        out.setdefault(ie.strip(), {}).setdefault(key.strip(), []).extend(
            [v for v in val.split("|") if v]
        )
    return out


EXTRACTOR_ARGS_DICT = parse_extractor_args(EXTRACTOR_ARGS)


def cookies_ok() -> bool:
    return bool(COOKIEFILE) and os.path.isfile(COOKIEFILE)


def ydl_extra() -> dict:
    extra: dict = {}
    if cookies_ok():
        extra["cookiefile"] = COOKIEFILE
    if EXTRACTOR_ARGS_DICT:
        extra["extractor_args"] = EXTRACTOR_ARGS_DICT
    return extra


def ydl_base(workdir: Path, hooks=None) -> dict:
    opts = {
        **ydl_extra(),
        "outtmpl": str(workdir / "%(title).70s [%(id)s].%(ext)s"),
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "noprogress": True,
        "consoletitle": False,
        "socket_timeout": 30,
        "retries": 5,
        "fragment_retries": 5,
        "overwrites": True,
    }
    if hooks:
        opts["progress_hooks"] = hooks
    if FFMPEG:
        opts["ffmpeg_location"] = FFMPEG
    return opts


def extract_info(url: str) -> dict:
    opts = {
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "skip_download": True,
        "socket_timeout": 30,
        **ydl_extra(),
    }
    with yt_dlp.YoutubeDL(opts) as ydl:
        return ydl.extract_info(url, download=False)


class Progress:
    """Collects real numbers from yt-dlp and pushes them onto the SSE queue."""

    def __init__(self, q: "queue.Queue[dict]", lo: float = 0.0, hi: float = 100.0):
        self.q = q
        self.lo, self.hi = lo, hi
        self.files: dict[str, list[int]] = {}
        self.last = 0.0
        self.last_emit = 0.0

    # -- yt-dlp hook -------------------------------------------------------
    def hook(self, d: dict) -> None:
        status = d.get("status")
        if status == "downloading":
            fn = d.get("filename") or d.get("tmpfilename") or "stream"
            got = d.get("downloaded_bytes") or 0
            tot = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
            self.files[fn] = [got, tot or self.files.get(fn, [0, 0])[1]]
            done = sum(v[0] for v in self.files.values())
            total = sum(v[1] for v in self.files.values())
            if total > 0:
                pct = done / total * 100.0
            else:
                pct = self._pct_from_str(d.get("_percent_str"))
            speed = d.get("speed") or 0
            eta = d.get("eta")
            if eta is None and speed and total:
                eta = max(0, int((total - done) / speed))
            self.push(pct, "download", done, total or None, speed, eta or 0)
        elif status == "finished":
            fn = d.get("filename") or ""
            if fn in self.files and self.files[fn][0]:
                self.files[fn][1] = self.files[fn][0] or self.files[fn][1]
            self.push(100.0, "download", None, None, 0, 0, force=True)

    @staticmethod
    def _pct_from_str(s):
        m = re.search(r"([\d.]+)%", s or "")
        return float(m.group(1)) if m else None

    # -- ffmpeg phase ------------------------------------------------------
    def push(self, pct, stage, done=None, total=None, speed=0, eta=0, force=False):
        now = time.time()
        if pct is not None:
            pct = max(0.0, min(100.0, float(pct)))
            if pct < self.last:  # never go backwards
                pct = self.last
            self.last = pct
        if not force and now - self.last_emit < 0.08:
            return
        self.last_emit = now
        overall = None if pct is None else self.lo + (self.hi - self.lo) * pct / 100.0
        self.q.put(
            {
                "type": "progress",
                "percent": None if overall is None else round(overall, 1),
                "stage": stage,
                "downloaded": done,
                "total": total,
                "speed": round(speed or 0),
                "eta": int(eta or 0),
            }
        )


def run_ffmpeg(args: list[str]):
    """Start ffmpeg, capturing stderr into a temp file; returns (proc, err_file)."""
    err_file = tempfile.TemporaryFile(mode="w+", errors="ignore")
    cmd = [
        FFMPEG,
        "-nostdin",
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-progress",
        "pipe:1",
        "-nostats",
        *args,
    ]
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=err_file,
        text=True,
        bufsize=1,
        errors="ignore",
    )
    return proc, err_file


def ffmpeg_percent(
    prog: Progress, args: list[str], duration: float, stage: str, lo: float, hi: float
) -> None:
    """Run ffmpeg while translating out_time_us into a real percentage."""
    if not FFMPEG:
        raise RuntimeError("ffmpeg tidak ditemukan. Install ffmpeg dulu.")
    proc, err_file = run_ffmpeg(args)
    total_us = max(float(duration or 0), 0.0) * 1_000_000
    cur = 0
    last = 0.0
    assert proc.stdout is not None
    for line in proc.stdout:
        line = line.strip()
        if not line or "=" not in line:
            continue
        key, _, val = line.partition("=")
        if key in ("out_time_us", "out_time_ms"):
            try:
                cur = int(val)
            except ValueError:
                cur = 0
        elif key == "progress":
            if total_us and cur:
                pct = min(100.0, cur / total_us * 100.0)
                now = time.time()
                if pct >= 100 or now - last > 0.08:
                    last = now
                    prog.push(pct, stage, None, None, 0, 0)
    proc.stdout.close()
    code = proc.wait()
    if code != 0:
        err_file.seek(0)
        tail = err_file.read()[:400].strip()
        err_file.close()
        raise RuntimeError(f"ffmpeg gagal (kode {code}). {tail}")
    err_file.close()


# --------------------------------------------------------------------------
# job pipeline
# --------------------------------------------------------------------------
def job_worker(job_id: str, url: str, mode: str, quality: str, q: "queue.Queue[dict]"):
    workdir = DOWNLOAD_DIR / job_id
    try:
        workdir.mkdir(parents=True, exist_ok=True)
        q.put({"type": "stage", "stage": "prepare", "percent": 0})

        info = extract_info(url)
        duration = float(info.get("duration") or 0)
        title = info.get("title") or "media"
        q.put({"type": "meta", "title": title, "duration": duration})

        if mode == "mp3":
            prog = Progress(q, 0.0, 82.0)
            raw = download_audio(url, workdir, prog)
            final = convert_mp3(raw, workdir, duration, quality, prog)
            mime = "audio/mpeg"
        else:
            prog = Progress(q, 0.0, 88.0)
            merged = download_video(url, workdir, quality, prog)
            final = finalize_mp4(merged, workdir, duration, prog)
            mime = "video/mp4"

        size = final.stat().st_size
        name = final.name
        with JOBS_LOCK:
            JOBS[job_id] = {"dir": str(workdir), "file": name}
        q.put(
            {
                "type": "done",
                "name": name,
                "size": size,
                "mime": mime,
                "url": f"/f/{job_id}/{quote(name)}",
            }
        )
    except Exception as exc:  # noqa: BLE001
        q.put({"type": "error", "message": clean_error(exc)})
    finally:
        q.put(None)


def download_audio(url: str, workdir: Path, prog: Progress) -> Path:
    opts = ydl_base(workdir, hooks=[prog.hook])
    opts["format"] = "bestaudio/best"
    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(url, download=True)
        path = ydl.prepare_filename(info)
    return Path(path)


def download_video(url: str, workdir: Path, quality: str, prog: Progress) -> Path:
    if quality in ("best", "0", ""):
        # H.264 first (plays everywhere), then anything yt-dlp can mux into mp4
        fmt = ("bv*[vcodec^=avc1][ext=mp4]+ba[ext=m4a]/"
               "bv*[ext=mp4]+ba[ext=m4a]/bv*+ba/b")
    else:
        h = int(quality)
        fmt = (
            f"bv*[height<={h}][vcodec^=avc1][ext=mp4]+ba[ext=m4a]/"
            f"bv*[height<={h}][ext=mp4]+ba[ext=m4a]/"
            f"bv*[height<={h}]+ba/b[height<={h}]/bv*+ba/b"
        )
    opts = ydl_base(workdir, hooks=[prog.hook])
    opts["format"] = fmt
    opts["merge_output_format"] = "mp4"
    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(url, download=True)
        req = info.get("requested_downloads") or []
        path = req[0].get("filepath") if req else ydl.prepare_filename(info)
    return Path(path)


def has_audio_stream(path: Path) -> bool:
    """True kalau file punya minimal satu stream audio (ffmpeg -i non-destruktif)."""
    if not FFMPEG:
        return True  # biarkan ffmpeg yang nanti protes
    try:
        p = subprocess.run(
            [FFMPEG, "-nostdin", "-hide_banner", "-i", str(path)],
            capture_output=True, text=True, errors="ignore", timeout=60,
        )
        return "Audio:" in (p.stderr or "")
    except Exception:  # noqa: BLE001
        return True


def convert_mp3(
    src: Path, workdir: Path, duration: float, bitrate: str, prog: Progress
) -> Path:
    bitrate = re.sub(r"\D", "", bitrate or "192") or "192"
    out = workdir / f"{safe_name(src.stem)}.mp3"
    if not has_audio_stream(src):
        try:
            src.unlink(missing_ok=True)
        except OSError:
            pass
        raise RuntimeError(
            "Video ini tidak punya track audio, jadi nggak bisa dijadikan MP3. "
            "Coba pilih MP4 atau pakai video lain."
        )
    conv = Progress(prog.q, 82.0, 100.0)
    conv.push(0.0, "convert", None, None, 0, 0, force=True)
    ffmpeg_percent(
        conv,
        ["-i", str(src), "-vn", "-acodec", "libmp3lame", "-b:a", f"{bitrate}k",
         "-id3v2_version", "3", "-map_metadata", "0", str(out)],
        duration,
        "convert",
        0.0,
        100.0,
    )
    try:
        src.unlink(missing_ok=True)
    except OSError:
        pass
    if not out.exists() or out.stat().st_size == 0:
        raise RuntimeError("Hasil konversi MP3 kosong.")
    return out


def finalize_mp4(src: Path, workdir: Path, duration: float, prog: Progress) -> Path:
    out = workdir / f"{safe_name(src.stem)}.mp4"
    # Skip the remux for very large files (it is only a faststart optimisation).
    try:
        if src.stat().st_size > 900 * 1024 * 1024:
            if src.resolve() != out.resolve():
                src.rename(out)
            return out
    except OSError:
        return src
    fin = Progress(prog.q, 88.0, 100.0)
    fin.push(0.0, "finalize", None, None, 0, 0, force=True)
    ffmpeg_percent(
        fin,
        ["-i", str(src), "-c", "copy", "-movflags", "+faststart", str(out)],
        duration,
        "finalize",
        0.0,
        100.0,
    )
    if out.exists() and out.resolve() != src.resolve():
        try:
            src.unlink(missing_ok=True)
        except OSError:
            pass
    return out if out.exists() else src


def sse_stream(q: "queue.Queue[dict]"):
    try:
        while True:
            try:
                item = q.get(timeout=15)
            except queue.Empty:
                yield ": keep-alive\n\n"
                continue
            if item is None:
                break
            kind = item.pop("type")
            yield f"event: {kind}\ndata: {json.dumps(item)}\n\n"
            if kind in ("done", "error"):
                break
    finally:
        pass


def cleanup_old(max_age: int = KEEP_SECONDS) -> None:
    now = time.time()
    try:
        for d in DOWNLOAD_DIR.iterdir():
            if not d.is_dir():
                continue
            try:
                if now - d.stat().st_mtime > max_age:
                    shutil.rmtree(d, ignore_errors=True)
            except OSError:
                continue
    except OSError:
        pass


def janitor():
    """Background sweeper: delete finished downloads that are too old."""
    while True:
        time.sleep(600)
        try:
            cleanup_old()
        except Exception:  # noqa: BLE001
            pass


cleanup_old()
threading.Thread(target=janitor, daemon=True).start()


# --------------------------------------------------------------------------
# routes
# --------------------------------------------------------------------------
@app.get("/")
def index():
    return render_template("index.html", ffmpeg=bool(FFMPEG))


@app.get("/healthz")
def healthz():
    """Liveness probe for Render health checks + ping bots. No network calls."""
    ffmpeg_ok = bool(FFMPEG)
    status = 200 if ffmpeg_ok else 503
    return (
        jsonify(
            {
                "ok": ffmpeg_ok,
                "status": "ready" if ffmpeg_ok else "ffmpeg missing",
                "ffmpeg": FFMPEG,
                "version": VERSION,
                "uptime": round(time.time() - START_TIME),
                "jobs": len(JOBS),
                "dir": str(DOWNLOAD_DIR),
                "cookies": cookies_ok(),
                "yt_dlp": yt_dlp.version.__version__,
            }
        ),
        status,
    )


@app.get("/ping")
def ping():
    """Tiniest possible probe (plain text) for cron/uptime services."""
    return Response("pong", mimetype="text/plain", headers={"Cache-Control": "no-store"})


@app.post("/api/info")
def api_info():
    data = request.get_json(silent=True) or {}
    url = (data.get("url") or "").strip()
    if not re.match(r"^https?://", url):
        return jsonify({"error": "URL tidak valid."}), 400
    try:
        info = extract_info(url)
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": clean_error(exc)}), 400

    thumb, best_w = None, -1
    for t in info.get("thumbnails") or []:
        w = t.get("width") or 0
        if w >= best_w and t.get("url"):
            best_w, thumb = w, t["url"]
    thumb = thumb or info.get("thumbnail")

    heights = sorted(
        {
            int(f["height"])
            for f in (info.get("formats") or [])
            if f.get("height") and f.get("vcodec") not in (None, "none")
            and int(f["height"]) <= 2160
        },
        reverse=True,
    )
    return jsonify(
        {
            "id": info.get("id"),
            "title": info.get("title"),
            "uploader": info.get("uploader") or info.get("channel") or "",
            "duration": info.get("duration"),
            "duration_string": info.get("duration_string"),
            "view_count": info.get("view_count"),
            "webpage_url": info.get("webpage_url") or url,
            "thumbnail": thumb,
            "thumb": f"/api/thumb?u={quote(thumb or '')}" if thumb else None,
            "heights": heights[:8],
        }
    )


@app.get("/api/thumb")
def api_thumb():
    u = request.args.get("u", "")
    host = urlparse(u).netloc.lower()
    if not u.startswith("http") or not (
        host.endswith("ytimg.com")
        or host.endswith("youtube.com")
        or "googlevideo" in host
        or host.endswith("ggpht.com")
    ):
        return Response("host tidak diizinkan", status=400)
    try:
        req = urllib.request.Request(
            u, headers={"User-Agent": "Mozilla/5.0", "Accept": "image/*"}
        )
        with urllib.request.urlopen(req, timeout=20) as r:
            data = r.read()
            ctype = r.headers.get("Content-Type", "image/jpeg")
    except Exception:
        return Response("gagal mengambil thumbnail", status=502)
    return Response(
        data, mimetype=ctype, headers={"Cache-Control": "public, max-age=86400"}
    )


@app.post("/api/download")
def api_download():
    data = request.get_json(silent=True) or {}
    url = (data.get("url") or "").strip()
    mode = "mp3" if data.get("mode") == "mp3" else "mp4"
    quality = str(data.get("quality") or ("192" if mode == "mp3" else "720"))
    if not re.match(r"^https?://", url):
        return jsonify({"error": "URL tidak valid."}), 400
    if not FFMPEG:
        return jsonify({"error": "ffmpeg tidak ditemukan di server."}), 500

    cleanup_old()
    job_id = uuid.uuid4().hex[:12]
    q: "queue.Queue[dict]" = queue.Queue()
    threading.Thread(
        target=job_worker, args=(job_id, url, mode, quality, q), daemon=True
    ).start()
    return Response(
        sse_stream(q),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/f/<job_id>/<path:name>")
def serve_file(job_id: str, name: str):
    if not JOB_ID_RE.match(job_id):
        return Response("not found", status=404)
    d = DOWNLOAD_DIR / job_id
    if not d.is_dir():
        return Response("file sudah kedaluwarsa", status=404)
    return send_from_directory(d, name, as_attachment=True)


atexit.register(lambda: shutil.rmtree(DOWNLOAD_DIR, ignore_errors=True))

if __name__ == "__main__":
    if not FFMPEG:
        print("PERINGATAN: ffmpeg tidak ditemukan. Install ffmpeg atau: pip install imageio-ffmpeg")
    print(f"Download folder : {DOWNLOAD_DIR}")
    print(f"yt-dlp          : {yt_dlp.version.__version__}")
    print(f"cookies         : {COOKIEFILE or '(tidak dipakai)'}"
          f"{' [file ketemu]' if cookies_ok() else ' [FILE TIDAK KETEMU]' if COOKIEFILE else ''}")
    print(f"Jalankan        : http://0.0.0.0:{PORT}")
    app.run(host="0.0.0.0", port=PORT, debug=False, threaded=True)

from __future__ import annotations

import argparse
import base64
import json
import os
import random
import signal
import ssl
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

DEFAULT_INTERVAL = 600       # 10 menit -> aman di bawah batas 15 menit Render
MAX_INTERVAL = 840           # 14 menit: jangan lebih dari ini
DEFAULT_TIMEOUT = 60         # cold start bisa 30-60 detik
COLD_START_THRESHOLD = 15.0  # respon > 15 detik = habis cold start
_RUNNING = True


def log(msg: str, logfile=None) -> None:
    stamp = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{stamp}] {msg}"
    print(line, flush=True)
    if logfile:
        try:
            with open(logfile, "a", encoding="utf-8") as fh:
                fh.write(line + "\n")
        except OSError:
            pass


def ping(url: str, user: str | None, password: str | None, timeout: int, logfile):
    """Ping once. Returns (ok, detail, elapsed_seconds)."""
    req = urllib.request.Request(url, method="GET")
    req.add_header("User-Agent", "keep-alive/1.1 (+render-ping-bot)")
    req.add_header("Cache-Control", "no-cache")
    if user and password:
        token = base64.b64encode(f"{user}:{password}".encode()).decode()
        req.add_header("Authorization", f"Basic {token}")

    ctx = ssl.create_default_context()
    start = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
            body = resp.read(2048).decode("utf-8", "replace")
            code = resp.status
    except urllib.error.HTTPError as e:  # 4xx/5xx
        return False, f"HTTP {e.code}", time.time() - start
    except Exception as e:  # noqa: BLE001  (timeout, DNS, SSL, connection reset)
        return False, f"{type(e).__name__}: {e}", time.time() - start

    elapsed = time.time() - start
    if code >= 400:
        return False, f"HTTP {code}", elapsed

    detail = f"HTTP {code} dalam {elapsed:.2f}s"
    try:  # /healthz ngasih JSON, tampilin ringkasannya
        data = json.loads(body)
        if isinstance(data, dict):
            bits = [f"status={data.get('status')}"]
            if data.get("uptime") is not None:
                bits.append(f"uptime={data['uptime']}s")
            if data.get("jobs") is not None:
                bits.append(f"jobs={data['jobs']}")
            detail += " | " + " ".join(bits)
            if data.get("ok") is False:
                return False, detail + " | healthz bilang nggak OK", elapsed
    except Exception:  # noqa: BLE001 - body bukan JSON (mis. /ping -> "pong")
        detail += ' | body="' + body.strip()[:40] + '"'
    return True, detail, elapsed


def main() -> int:
    ap = argparse.ArgumentParser(description="Ping bot buat jaga service Render tetap hidup.")
    ap.add_argument("--url", default=os.environ.get("PING_URL", ""),
                    help="URL target, bisa beberapa dipisah koma (contoh: https://app.onrender.com/healthz)")
    ap.add_argument("--interval", type=int, default=int(os.environ.get("PING_INTERVAL", DEFAULT_INTERVAL)),
                    help=f"Jeda antar ping dalam detik (default {DEFAULT_INTERVAL}, maks {MAX_INTERVAL})")
    ap.add_argument("--path", default="/healthz", help="Path health (default /healthz)")
    ap.add_argument("--user", default=os.environ.get("PING_USER", ""), help="Basic auth user (opsional)")
    ap.add_argument("--pass", dest="password", default=os.environ.get("PING_PASS", ""),
                    help="Basic auth password (opsional)")
    ap.add_argument("--timeout", type=int, default=int(os.environ.get("PING_TIMEOUT", DEFAULT_TIMEOUT)),
                    help=f"Timeout tiap request (default {DEFAULT_TIMEOUT}s)")
    ap.add_argument("--log", default=os.environ.get("PING_LOG", ""), help="File log (opsional)")
    ap.add_argument("--once", action="store_true", help="Ping sekali lalu keluar (cocok buat cron)")
    args = ap.parse_args()

    urls = [u.strip() for u in args.url.split(",") if u.strip()]
    if not urls:
        ap.error("--url wajib diisi (atau set env PING_URL), contoh: https://app.onrender.com")

    # lengkapi path kalau user cuma ngasih domain
    fixed = []
    for u in urls:
        if not u.startswith("http"):
            u = "https://" + u
        if u.rstrip("/").count("/") == 2:  # https://host -> tambahin path health
            u = u.rstrip("/") + args.path
        fixed.append(u)
    urls = fixed

    interval = min(max(60, args.interval), MAX_INTERVAL)
    if args.interval > MAX_INTERVAL:
        log(f"PERINGATAN: interval {args.interval}s diturunkin ke {MAX_INTERVAL}s "
            f"(Render tidur setelah 15 menit).", args.log)

    def stop(signum, _frame):
        global _RUNNING
        _RUNNING = False
        log("Berhenti (sinyal diterima). Bye!", args.log)

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    log(f"Ping bot jalan. Target: {', '.join(urls)}", args.log)
    log(f"Interval: {interval}s ({interval/60:.1f} menit) | timeout: {args.timeout}s", args.log)

    fails = 0
    while _RUNNING:
        for u in urls:
            if not _RUNNING:
                break
            ok, detail, elapsed = ping(u, args.user, args.password, args.timeout, args.log)
            if ok:
                fails = 0
                extra = "  <- cold start!" if elapsed > COLD_START_THRESHOLD else ""
                log(f"OK   {u}  {detail}{extra}", args.log)
            else:
                fails += 1
                backoff = min(300, 30 * (2 ** (fails - 1)))  # 30s, 60s, 120s, 300s...
                log(f"GAGAL {u}  {detail} (gagal ke-{fails}, retry {backoff}s)", args.log)
                if not args.once:
                    _sleep(backoff)
        if args.once:
            break
        if _RUNNING:
            _sleep(interval)
    return 0


def _sleep(seconds: int) -> None:
    """Tidur dengan jitter supaya nggak selalu tepat di menit yg sama."""
    jitter = random.uniform(-0.05, 0.05) * seconds
    end = time.time() + max(1, seconds + jitter)
    while _RUNNING and time.time() < end:
        time.sleep(min(1.0, max(0.0, end - time.time())))


if __name__ == "__main__":
    sys.exit(main())

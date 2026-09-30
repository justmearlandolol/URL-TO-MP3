from flask import Flask, render_template, request, jsonify
import yt_dlp
import os

app = Flask(__name__)

# Folder tempat menyimpan hasil download
DOWNLOAD_FOLDER = 'downloads'
if not os.path.exists(DOWNLOAD_FOLDER):
    os.makedirs(DOWNLOAD_FOLDER)

def download_audio(url):
    ydl_opts = {
        'format': 'bestaudio/best',
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '192',
        }],
        # Simpan di folder downloads
        'outtmpl': f'{DOWNLOAD_FOLDER}/%(title)s.%(ext)s',
        'quiet': True,
        'no_warnings': True,
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        ydl.download([url])

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/download', methods=['POST'])
def handle_download():
    data = request.json
    url = data.get('url')

    if not url:
        return jsonify({'status': 'error', 'message': 'URL tidak boleh kosong!'}), 400

    try:
        download_audio(url)
        return jsonify({'status': 'success', 'message': 'Success! The video has already been installed as an MP3.'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)


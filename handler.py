import runpod
import subprocess
import requests
import tempfile
import os
import dropbox
from datetime import datetime

def handler(event):
    try:
        print("Début du traitement")
        video_url = event['input']['video_url']
        cuts = event['input']['cuts']
        dropbox_folder = event['input'].get('dropbox_folder', '/processed_videos/')
        dropbox_token = event['input']['dropbox_token']
        
        print(f"URL vidéo: {video_url}")
        print(f"Découpes: {cuts}")
        print(f"Upload vers: {dropbox_folder}")
        
        # Télécharge la vidéo
        print("Téléchargement de la vidéo...")
        response = requests.get(video_url, stream=True)
        with tempfile.NamedTemporaryFile(suffix='.mp4', delete=False) as temp_video:
            for chunk in response.iter_content(chunk_size=8192):
                temp_video.write(chunk)
            input_path = temp_video.name
        
        print(f"Vidéo téléchargée: {input_path}")
        
        # Découpe avec FFMPEG
        output_path = '/tmp/output.mp4'  # CORRECTION: Définir output_path ici
        start = cuts[0]['start']
        end = cuts[0]['end']
        
        print(f"Découpe de {start}s à {end}s")
        
        cmd = ['ffmpeg', '-i', input_path, '-ss', str(start), '-to', str(end), '-c', 'copy', '-y', output_path]
        
        print(f"Commande FFMPEG: {' '.join(cmd)}")
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"Erreur FFMPEG: {result.stderr}")
            return {"error": f"FFMPEG failed: {result.stderr}"}
        
        file_size = os.path.getsize(output_path) if os.path.exists(output_path) else 0
        print(f"Traitement terminé, taille: {file_size} bytes")
        
        # Upload vers Dropbox
        print(f"Upload vers Dropbox: {dropbox_folder}")
        
        dbx = dropbox.Dropbox(dropbox_token)
        
        # Nom de fichier avec timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"video_cut_{timestamp}.mp4"
        dropbox_path = f"{dropbox_folder.rstrip('/')}/{filename}"
        
        # Upload
        with open(output_path, 'rb') as f:
            dbx.files_upload(f.read(), dropbox_path, mode=dropbox.files.WriteMode.overwrite)
        
        print(f"Fichier uploadé: {dropbox_path}")
        
        # Créer lien de partage
        try:
            shared_link = dbx.sharing_create_shared_link(dropbox_path)
            download_url = shared_link.url
        except Exception as e:
            print(f"Erreur création lien: {e}")
            download_url = f"File uploaded to {dropbox_path}"
        
        return {
            "success": True,
            "message": "Video processed and uploaded to Dropbox",
            "dropbox_path": dropbox_path,
            "download_url": download_url,
            "output_size_mb": round(file_size / 1024 / 1024, 2),
            "duration_cut": end - start
        }
        
    except Exception as e:
        print(f"Erreur: {str(e)}")
        return {"error": str(e)}

if __name__ == "__main__":
    runpod.serverless.start({"handler": handler})

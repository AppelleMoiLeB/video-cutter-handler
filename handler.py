import runpod
import subprocess
import requests
import tempfile
import os
import dropbox

def handler(event):
    try:
        print("Début du traitement")
        video_url = event['input']['video_url']
        cuts = event['input']['cuts']
        dropbox_folder = event['input'].get('dropbox_folder', '/processed_videos/')  # Dossier par défaut
        dropbox_token = event['input']['dropbox_token']  # Token depuis Make.com
        
        print(f"Upload vers: {dropbox_folder}")
        
        # ... code de téléchargement et découpe existant ...
        
        # Après la découpe FFMPEG réussie
        print(f"Upload vers Dropbox: {dropbox_folder}")
        
        # Connexion Dropbox
        dbx = dropbox.Dropbox(dropbox_token)
        
        # Nom de fichier avec timestamp
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"video_cut_{timestamp}.mp4"
        dropbox_path = f"{dropbox_folder.rstrip('/')}/{filename}"
        
        # Upload
        with open(output_path, 'rb') as f:
            dbx.files_upload(f.read(), dropbox_path, mode=dropbox.files.WriteMode.overwrite)
        
        # Créer lien de partage
        try:
            shared_link = dbx.sharing_create_shared_link(dropbox_path)
            download_url = shared_link.url
        except:
            download_url = f"File uploaded to {dropbox_path}"
        
        return {
            "success": True,
            "message": "Video processed and uploaded to Dropbox",
            "dropbox_path": dropbox_path,
            "download_url": download_url,
            "output_size_mb": round(file_size / 1024 / 1024, 2)
        }
        
    except Exception as e:
        print(f"Erreur: {str(e)}")
        return {"error": str(e)}

if __name__ == "__main__":
    runpod.serverless.start({"handler": handler})

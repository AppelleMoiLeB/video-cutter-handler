import runpod
import subprocess
import requests
import tempfile
import os

def handler(event):
    try:
        print("Début du traitement")
        video_url = event['input']['video_url']
        cuts = event['input']['cuts']
        
        print(f"URL vidéo: {video_url}")
        print(f"Découpes: {cuts}")
        
        # Télécharge la vidéo
        print("Téléchargement de la vidéo...")
        response = requests.get(video_url, stream=True)
        with tempfile.NamedTemporaryFile(suffix='.mp4', delete=False) as temp_video:
            for chunk in response.iter_content(chunk_size=8192):
                temp_video.write(chunk)
            input_path = temp_video.name
        
        print(f"Vidéo téléchargée: {input_path}")
        
        # Découpe avec FFMPEG
        output_path = '/tmp/output.mp4'
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
        
        return {
            "success": True, 
            "message": "Video processed successfully",
            "output_size_mb": round(file_size / 1024 / 1024, 2),
            "duration_cut": end - start
        }
        
    except Exception as e:
        print(f"Erreur: {str(e)}")
        return {"error": str(e)}

if __name__ == "__main__":
    runpod.serverless.start({"handler": handler})

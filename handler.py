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
        cuts_data = event['input']['cuts']
        dropbox_folder = event['input'].get('dropbox_folder', '/processed_videos/')
        dropbox_token = event['input']['dropbox_token']
        custom_filename = event['input'].get('filename', None)
        
        print(f"URL vidéo: {video_url}")
        print(f"Données cuts reçues: {cuts_data}")
        print(f"Upload vers: {dropbox_folder}")
        print(f"Nom fichier personnalisé: {custom_filename}")
        
        # Traitement du nouveau format JSON consolidé
        if isinstance(cuts_data, dict) and 'cuts' in cuts_data:
            segments = cuts_data['cuts']
        elif isinstance(cuts_data, list):
            segments = cuts_data
        else:
            # Fallback pour ancien format
            if isinstance(cuts_data, dict) and 'segments' in cuts_data:
                segments = cuts_data['segments']
            else:
                segments = cuts_data
        
        print(f"Segments extraits: {len(segments) if isinstance(segments, list) else 'format invalide'}")
        
        # Conversion des segments en format numérique (millisecondes vers secondes)
        processed_segments = []
        for segment in segments:
            try:
                # Les timestamps du JSON consolidé sont déjà en millisecondes
                start_ms = float(segment['start'])
                end_ms = float(segment['end'])
                
                # Convertir en secondes pour FFMPEG
                start_sec = start_ms / 1000
                end_sec = end_ms / 1000
                
                processed_segments.append({"start": start_sec, "end": end_sec})
                print(f"Segment traité: {start_sec}s → {end_sec}s (type: {segment.get('type', 'unknown')})")
            except (ValueError, KeyError) as e:
                print(f"Erreur conversion segment {segment}: {e}")
                continue
        
        if not processed_segments:
            return {"error": "Aucun segment valide trouvé"}
        
        print(f"Segments finaux: {processed_segments}")
        
        # Télécharge la vidéo
        print("Téléchargement de la vidéo...")
        response = requests.get(video_url, stream=True)
        response.raise_for_status()
        
        with tempfile.NamedTemporaryFile(suffix='.mp4', delete=False) as temp_video:
            for chunk in response.iter_content(chunk_size=8192):
                temp_video.write(chunk)
            input_path = temp_video.name
        
        print(f"Vidéo téléchargée: {input_path}")
        
        # Création des filtres FFMPEG pour concaténer tous les segments
        output_path = '/tmp/output.mp4'
        
        if len(processed_segments) == 1:
            # Un seul segment : découpe simple
            segment = processed_segments[0]
            cmd = [
                'ffmpeg', '-i', input_path,
                '-ss', str(segment['start']),
                '-to', str(segment['end']),
                '-c', 'copy', '-y', output_path
            ]
        else:
            # Plusieurs segments : utiliser filter_complex pour concaténation
            filter_parts = []
            
            for i, segment in enumerate(processed_segments):
                start = segment['start']
                duration = segment['end'] - segment['start']
                filter_parts.append(f"[0:v]trim=start={start}:duration={duration},setpts=PTS-STARTPTS[v{i}]")
                filter_parts.append(f"[0:a]atrim=start={start}:duration={duration},asetpts=PTS-STARTPTS[a{i}]")
            
            # Concaténer tous les segments
            video_inputs = ''.join([f"[v{i}]" for i in range(len(processed_segments))])
            audio_inputs = ''.join([f"[a{i}]" for i in range(len(processed_segments))])
            concat_filter = f"{video_inputs}concat=n={len(processed_segments)}:v=1:a=0[outv]"
            concat_filter += f";{audio_inputs}concat=n={len(processed_segments)}:v=0:a=1[outa]"
            
            full_filter = ';'.join(filter_parts) + ';' + concat_filter
            
            cmd = [
                'ffmpeg', '-i', input_path,
                '-filter_complex', full_filter,
                '-map', '[outv]', '-map', '[outa]',
                '-c:v', 'libx264', '-c:a', 'aac',
                '-y', output_path
            ]
        
        print(f"Commande FFMPEG: {' '.join(cmd)}")
        
        # Exécute FFMPEG
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"Erreur FFMPEG: {result.stderr}")
            return {"error": f"FFMPEG failed: {result.stderr}"}
        
        file_size = os.path.getsize(output_path) if os.path.exists(output_path) else 0
        print(f"Traitement terminé, taille: {file_size} bytes")
        
        # Upload vers Dropbox avec chunks et sécurité renforcée
        print(f"Upload vers Dropbox: {dropbox_folder}")
        
        dbx = dropbox.Dropbox(dropbox_token)
        
        # Génération de nom de fichier sécurisé
        if custom_filename:
            import re
            safe_filename = re.sub(r'[<>:"/\\|?*]', '_', custom_filename)
            if not safe_filename.lower().endswith('.mp4'):
                filename = f"{safe_filename}.mp4"
            else:
                filename = safe_filename
        else:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"video_cut_{timestamp}.mp4"
        
        # Vérification que le fichier n'existe pas déjà
        dropbox_path = f"{dropbox_folder.rstrip('/')}/{filename}"
        original_filename = filename
        counter = 1
        while True:
            try:
                dbx.files_get_metadata(dropbox_path)
                name_part = original_filename.rsplit('.', 1)[0]
                extension = original_filename.rsplit('.', 1)[1] if '.' in original_filename else 'mp4'
                filename = f"{name_part}_{counter}.{extension}"
                dropbox_path = f"{dropbox_folder.rstrip('/')}/{filename}"
                counter += 1
                print(f"Fichier existant détecté, nouveau nom: {filename}")
            except dropbox.exceptions.ApiError:
                print(f"Nom de fichier final: {filename}")
                break
        
        # Upload par chunks pour gros fichiers
        CHUNK_SIZE = 4 * 1024 * 1024  # 4MB par chunk
        
        with open(output_path, 'rb') as f:
            if file_size <= CHUNK_SIZE:
                print("Upload direct (fichier < 4MB)")
                contents = f.read()
                dbx.files_upload(
                    contents, 
                    dropbox_path, 
                    mode=dropbox.files.WriteMode.add,
                    autorename=True
                )
            else:
                print(f"Upload par chunks ({file_size} bytes, chunks de {CHUNK_SIZE} bytes)")
                
                first_chunk = f.read(CHUNK_SIZE)
                session_start_result = dbx.files_upload_session_start(first_chunk)
                cursor = dropbox.files.UploadSessionCursor(
                    session_id=session_start_result.session_id,
                    offset=f.tell()
                )
                
                print(f"Session démarrée: {session_start_result.session_id}")
                
                chunk_count = 1
                while f.tell() < file_size:
                    chunk_count += 1
                    remaining = file_size - f.tell()
                    
                    if remaining <= CHUNK_SIZE:
                        print(f"Upload chunk final {chunk_count} ({remaining} bytes)")
                        final_chunk = f.read(remaining)
                        commit_info = dropbox.files.CommitInfo(
                            path=dropbox_path, 
                            mode=dropbox.files.WriteMode.add,
                            autorename=True
                        )
                        result = dbx.files_upload_session_finish(final_chunk, cursor, commit_info)
                        dropbox_path = result.path_display
                        filename = dropbox_path.split('/')[-1]
                        break
                    else:
                        print(f"Upload chunk {chunk_count} ({CHUNK_SIZE} bytes)")
                        chunk_data = f.read(CHUNK_SIZE)
                        dbx.files_upload_session_append_v2(chunk_data, cursor)
                        cursor.offset = f.tell()
        
        print(f"Fichier uploadé avec succès: {dropbox_path}")
        
        # Créer lien de partage
        try:
            shared_link = dbx.sharing_create_shared_link(dropbox_path)
            download_url = shared_link.url
            print(f"Lien de partage créé: {download_url}")
        except Exception as e:
            print(f"Erreur création lien: {e}")
            download_url = f"File uploaded to {dropbox_path}"
        
        # Nettoyage des fichiers temporaires
        try:
            os.unlink(input_path)
            os.unlink(output_path)
        except:
            pass
        
        # Calcul de la durée totale conservée
        total_duration = sum(seg['end'] - seg['start'] for seg in processed_segments)
        
        return {
            "success": True,
            "message": "Video processed and uploaded to Dropbox safely",
            "dropbox_path": dropbox_path,
            "filename_used": filename,
            "download_url": download_url,
            "output_size_mb": round(file_size / 1024 / 1024, 2),
            "segments_processed": len(processed_segments),
            "total_duration_kept": round(total_duration, 2),
            "chunks_uploaded": chunk_count if file_size > CHUNK_SIZE else 1,
            "security_mode": "safe_upload_no_overwrite"
        }
        
    except Exception as e:
        print(f"Erreur: {str(e)}")
        return {"error": str(e)}

if __name__ == "__main__":
    runpod.serverless.start({"handler": handler})

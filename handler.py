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
        print(f"Découpes brutes: {cuts_data}")
        print(f"Upload vers: {dropbox_folder}")
        print(f"Nom fichier personnalisé: {custom_filename}")
        
        # Traitement des segments avec gestion robuste du format
        if isinstance(cuts_data, dict) and 'segments' in cuts_data:
            segments_to_remove = cuts_data['segments']
        else:
            segments_to_remove = cuts_data
        
        # Conversion des segments à SUPPRIMER en format numérique
        remove_segments = []
        for segment in segments_to_remove:
            start_str = str(segment['start']).rstrip('s')
            end_str = str(segment['end']).rstrip('s')
            
            try:
                start = float(start_str)
                end = float(end_str)
                remove_segments.append({"start": start, "end": end})
                print(f"Segment À SUPPRIMER: {start}s → {end}s")
            except ValueError as e:
                print(f"Erreur conversion segment {segment}: {e}")
                continue
        
        # Télécharge la vidéo pour connaître sa durée totale
        print("Téléchargement de la vidéo...")
        response = requests.get(video_url, stream=True)
        response.raise_for_status()
        
        with tempfile.NamedTemporaryFile(suffix='.mp4', delete=False) as temp_video:
            for chunk in response.iter_content(chunk_size=8192):
                temp_video.write(chunk)
            input_path = temp_video.name
        
        print(f"Vidéo téléchargée: {input_path}")
        
        # NOUVEAU: Obtenir la durée totale de la vidéo avec ffprobe
        duration_cmd = ['ffprobe', '-i', input_path, '-show_entries', 'format=duration', '-v', 'quiet', '-of', 'csv=p=0']
        duration_result = subprocess.run(duration_cmd, capture_output=True, text=True)
        
        if duration_result.returncode != 0:
            return {"error": "Impossible de déterminer la durée de la vidéo"}
        
        total_duration = float(duration_result.stdout.strip())
        print(f"Durée totale de la vidéo: {total_duration}s")
        
        # LOGIQUE INVERSÉE: Calculer les segments à GARDER
        remove_segments.sort(key=lambda x: x['start'])  # Trier par ordre chronologique
        
        keep_segments = []
        current_time = 0.0
        
        for remove_segment in remove_segments:
            remove_start = remove_segment['start']
            remove_end = remove_segment['end']
            
            # Ajouter le segment avant la suppression (si il y a du contenu)
            if current_time < remove_start:
                keep_segments.append({
                    "start": current_time,
                    "end": remove_start
                })
                print(f"Segment À GARDER: {current_time}s → {remove_start}s")
            
            # Avancer le curseur après le segment supprimé
            current_time = max(current_time, remove_end)
        
        # Ajouter le segment final (après la dernière suppression jusqu'à la fin)
        if current_time < total_duration:
            keep_segments.append({
                "start": current_time,
                "end": total_duration
            })
            print(f"Segment final À GARDER: {current_time}s → {total_duration}s")
        
        if not keep_segments:
            return {"error": "Aucun segment à garder après suppression des passages ratés"}
        
        print(f"Segments finaux à garder: {keep_segments}")
        
        # Création des filtres FFMPEG pour concaténer tous les segments À GARDER
        output_path = '/tmp/output.mp4'
        
        if len(keep_segments) == 1:
            # Un seul segment : découpe simple
            segment = keep_segments[0]
            cmd = [
                'ffmpeg', '-i', input_path,
                '-ss', str(segment['start']),
                '-to', str(segment['end']),
                '-c', 'copy', '-y', output_path
            ]
        else:
            # Plusieurs segments : utiliser filter_complex pour concaténation
            filter_parts = []
            
            for i, segment in enumerate(keep_segments):
                start = segment['start']
                duration = segment['end'] - segment['start']
                filter_parts.append(f"[0:v]trim=start={start}:duration={duration},setpts=PTS-STARTPTS[v{i}]")
                filter_parts.append(f"[0:a]atrim=start={start}:duration={duration},asetpts=PTS-STARTPTS[a{i}]")
            
            # Concaténer tous les segments
            video_inputs = ''.join([f"[v{i}]" for i in range(len(keep_segments))])
            audio_inputs = ''.join([f"[a{i}]" for i in range(len(keep_segments))])
            concat_filter = f"{video_inputs}concat=n={len(keep_segments)}:v=1:a=0[outv]"
            concat_filter += f";{audio_inputs}concat=n={len(keep_segments)}:v=0:a=1[outa]"
            
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
        
        # SÉCURITÉ: Génération de nom de fichier sécurisé
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
        
        # SÉCURITÉ: Vérifier si le fichier existe déjà et générer un nom unique
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
        
        # Upload par chunks pour gros fichiers avec mode sécurisé
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
        total_kept_duration = sum(seg['end'] - seg['start'] for seg in keep_segments)
        total_removed_duration = sum(seg['end'] - seg['start'] for seg in remove_segments)
        
        return {
            "success": True,
            "message": "Video processed with segments removed successfully",
            "dropbox_path": dropbox_path,
            "filename_used": filename,
            "download_url": download_url,
            "output_size_mb": round(file_size / 1024 / 1024, 2),
            "segments_removed": len(remove_segments),
            "segments_kept": len(keep_segments),
            "total_duration_kept": round(total_kept_duration, 2),
            "total_duration_removed": round(total_removed_duration, 2),
            "time_saved": f"{round(total_removed_duration, 2)}s",
            "chunks_uploaded": chunk_count if file_size > CHUNK_SIZE else 1,
            "logic": "inverted_to_keep_good_content"
        }
        
    except Exception as e:
        print(f"Erreur: {str(e)}")
        return {"error": str(e)}

if __name__ == "__main__":
    runpod.serverless.start({"handler": handler})

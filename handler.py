import runpod
import subprocess
import requests
import tempfile
import os
import dropbox
import json
from datetime import datetime

def invert_cuts_to_keeps(cuts, total_duration):
    """Convertit les cuts (à supprimer) en segments à garder"""
    if not cuts:
        return [{"start": 0, "end": total_duration}]
    
    keeps = []
    cuts_sorted = sorted(cuts, key=lambda x: x['start'])
    
    print(f"Cuts triés: {cuts_sorted}")
    print(f"Durée totale: {total_duration}")
    
    # Segment avant le premier cut
    if cuts_sorted[0]['start'] > 0.1:  # Si > 100ms
        keeps.append({"start": 0, "end": cuts_sorted[0]['start']})
        print(f"Segment initial: 0 → {cuts_sorted[0]['start']}")
    
    # Segments entre les cuts
    for i in range(len(cuts_sorted) - 1):
        start = cuts_sorted[i]['end']
        end = cuts_sorted[i + 1]['start']
        if end > start + 0.1:  # Garder seulement si > 100ms
            keeps.append({"start": start, "end": end})
            print(f"Segment entre cuts: {start} → {end}")
    
    # Segment après le dernier cut
    last_cut_end = cuts_sorted[-1]['end']
    if last_cut_end < total_duration - 0.1:
        keeps.append({"start": last_cut_end, "end": total_duration})
        print(f"Segment final: {last_cut_end} → {total_duration}")
    
    print(f"Segments à garder générés: {keeps}")
    return keeps

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
        
        # Conversion des segments en format numérique (cuts à supprimer)
        cuts_to_remove = []
        for segment in segments:
            try:
                # Normalisation des timestamps (gérer ms/s selon la valeur)
                start_val = float(segment['start'])
                end_val = float(segment['end'])
                
                # Si valeurs > 1000, probablement en millisecondes
                if start_val > 1000:
                    start_sec = start_val / 1000
                    end_sec = end_val / 1000
                else:
                    start_sec = start_val
                    end_sec = end_val
                
                cuts_to_remove.append({"start": start_sec, "end": end_sec})
                print(f"Cut à supprimer: {start_sec}s → {end_sec}s (type: {segment.get('type', 'unknown')})")
            except (ValueError, KeyError) as e:
                print(f"Erreur conversion segment {segment}: {e}")
                continue
        
        if not cuts_to_remove:
            return {"error": "Aucun cut valide trouvé"}
        
        # Télécharge la vidéo
        print("Téléchargement de la vidéo...")
        response = requests.get(video_url, stream=True)
        response.raise_for_status()
        
        with tempfile.NamedTemporaryFile(suffix='.mp4', delete=False) as temp_video:
            for chunk in response.iter_content(chunk_size=8192):
                temp_video.write(chunk)
            input_path = temp_video.name
        
        print(f"Vidéo téléchargée: {input_path}")
        
        # Obtenir la durée totale du fichier
        duration_cmd = ['ffprobe', '-v', 'quiet', '-show_entries', 'format=duration', '-of', 'csv=p=0', input_path]
        duration_result = subprocess.run(duration_cmd, capture_output=True, text=True)
        
        if duration_result.returncode != 0:
            print(f"Erreur obtention durée: {duration_result.stderr}")
            return {"error": f"Impossible d'obtenir la durée: {duration_result.stderr}"}
        
        total_duration = float(duration_result.stdout.strip())
        print(f"Durée totale fichier: {total_duration}s")
        
        # Convertir les cuts en segments à garder
        processed_segments = invert_cuts_to_keeps(cuts_to_remove, total_duration)
        
        if not processed_segments:
            return {"error": "Aucun segment à garder après inversion"}
        
        # Analyse du fichier pour détecter audio/vidéo
        probe_cmd = ['ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_streams', input_path]
        probe_result = subprocess.run(probe_cmd, capture_output=True, text=True)
        
        if probe_result.returncode != 0:
            print(f"Erreur ffprobe: {probe_result.stderr}")
            return {"error": f"Erreur analyse fichier: {probe_result.stderr}"}
        
        probe_data = json.loads(probe_result.stdout)
        has_video = any(stream['codec_type'] == 'video' for stream in probe_data['streams'])
        has_audio = any(stream['codec_type'] == 'audio' for stream in probe_data['streams'])
        
        print(f"Analyse fichier - Vidéo: {has_video}, Audio: {has_audio}")
        
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
            # Plusieurs segments : filtres selon type de média
            if has_video and has_audio:
                # Fichier vidéo avec audio
                filter_parts = []
                
                for i, segment in enumerate(processed_segments):
                    start = segment['start']
                    duration = segment['end'] - segment['start']
                    if duration < 0.1:  # Ignorer segments trop courts
                        print(f"Segment {i} ignoré (trop court): {duration}s")
                        continue
                    filter_parts.append(f"[0:v]trim=start={start}:duration={duration},setpts=PTS-STARTPTS[v{i}]")
                    filter_parts.append(f"[0:a]atrim=start={start}:duration={duration},asetpts=PTS-STARTPTS[a{i}]")
                
                if not filter_parts:
                    return {"error": "Tous les segments sont trop courts"}
                
                # Compter les segments valides
                valid_segments = len(filter_parts) // 2
                
                # Concaténer tous les segments
                video_inputs = ''.join([f"[v{i}]" for i in range(valid_segments)])
                audio_inputs = ''.join([f"[a{i}]" for i in range(valid_segments)])
                concat_filter = f"{video_inputs}concat=n={valid_segments}:v=1:a=0[outv]"
                concat_filter += f";{audio_inputs}concat=n={valid_segments}:v=0:a=1[outa]"
                
                full_filter = ';'.join(filter_parts) + ';' + concat_filter
                
                cmd = [
                    'ffmpeg', '-i', input_path,
                    '-filter_complex', full_filter,
                    '-map', '[outv]', '-map', '[outa]',
                    '-c:v', 'libx264', '-c:a', 'aac',
                    '-y', output_path
                ]
                
            elif has_audio and not has_video:
                # Fichier audio uniquement
                filter_parts = []
                
                for i, segment in enumerate(processed_segments):
                    start = segment['start']
                    duration = segment['end'] - segment['start']
                    if duration < 0.1:
                        continue
                    filter_parts.append(f"[0:a]atrim=start={start}:duration={duration},asetpts=PTS-STARTPTS[a{i}]")
                
                if not filter_parts:
                    return {"error": "Tous les segments audio sont trop courts"}
                
                valid_segments = len(filter_parts)
                audio_inputs = ''.join([f"[a{i}]" for i in range(valid_segments)])
                concat_filter = f"{audio_inputs}concat=n={valid_segments}:v=0:a=1[outa]"
                
                full_filter = ';'.join(filter_parts) + ';' + concat_filter
                
                cmd = [
                    'ffmpeg', '-i', input_path,
                    '-filter_complex', full_filter,
                    '-map', '[outa]',
                    '-c:a', 'aac', '-y', output_path
                ]
                
            elif has_video and not has_audio:
                # Fichier vidéo sans audio
                filter_parts = []
                
                for i, segment in enumerate(processed_segments):
                    start = segment['start']
                    duration = segment['end'] - segment['start']
                    if duration < 0.1:
                        continue
                    filter_parts.append(f"[0:v]trim=start={start}:duration={duration},setpts=PTS-STARTPTS[v{i}]")
                
                if not filter_parts:
                    return {"error": "Tous les segments vidéo sont trop courts"}
                
                valid_segments = len(filter_parts)
                video_inputs = ''.join([f"[v{i}]" for i in range(valid_segments)])
                concat_filter = f"{video_inputs}concat=n={valid_segments}:v=1:a=0[outv]"
                
                full_filter = ';'.join(filter_parts) + ';' + concat_filter
                
                cmd = [
                    'ffmpeg', '-i', input_path,
                    '-filter_complex', full_filter,
                    '-map', '[outv]',
                    '-c:v', 'libx264', '-y', output_path
                ]
            else:
                return {"error": "Aucun stream audio ou vidéo détecté"}
        
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
                        result_upload = dbx.files_upload_session_finish(final_chunk, cursor, commit_info)
                        dropbox_path = result_upload.path_display
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
        total_duration_kept = sum(seg['end'] - seg['start'] for seg in processed_segments)
        
        return {
            "success": True,
            "message": "Video processed and uploaded to Dropbox safely",
            "dropbox_path": dropbox_path,
            "filename_used": filename,
            "download_url": download_url,
            "output_size_mb": round(file_size / 1024 / 1024, 2),
            "segments_processed": len(processed_segments),
            "total_duration_kept": round(total_duration_kept, 2),
            "chunks_uploaded": chunk_count if file_size > CHUNK_SIZE else 1,
            "media_type": f"video: {has_video}, audio: {has_audio}",
            "cuts_removed": len(cuts_to_remove),
            "security_mode": "safe_upload_no_overwrite"
        }
        
    except Exception as e:
        print(f"Erreur: {str(e)}")
        return {"error": str(e)}

if __name__ == "__main__":
    runpod.serverless.start({"handler": handler})

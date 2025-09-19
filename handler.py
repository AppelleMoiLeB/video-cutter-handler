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
    
    print(f"=== DEBUG INVERSION ===")
    print(f"Durée totale: {total_duration}")
    print(f"Nombre de cuts: {len(cuts_sorted)}")
    
    for i, cut in enumerate(cuts_sorted):
        print(f"Cut {i}: {cut['start']} → {cut['end']}")
    
    current_pos = 0
    
    for i, cut in enumerate(cuts_sorted):
        print(f"\nTraitement cut {i}: {cut['start']} → {cut['end']}")
        print(f"Position actuelle: {current_pos}")
        
        # Ajouter segment avant ce cut
        gap_duration = cut['start'] - current_pos
        print(f"Gap avant cut: {gap_duration}s")
        
        if current_pos < cut['start'] and gap_duration > 0.1:
            segment = {"start": current_pos, "end": cut['start']}
            keeps.append(segment)
            print(f"✓ Segment ajouté: {current_pos} → {cut['start']} (durée: {gap_duration}s)")
        else:
            print(f"✗ Gap ignoré (trop court ou négatif)")
        
        # Avancer après ce cut
        current_pos = max(current_pos, cut['end'])
        print(f"Nouvelle position: {current_pos}")
    
    # Segment final
    final_gap = total_duration - current_pos
    print(f"\nSegment final potentiel: {current_pos} → {total_duration} (durée: {final_gap}s)")
    
    if current_pos < total_duration - 0.1:
        segment = {"start": current_pos, "end": total_duration}
        keeps.append(segment)
        print(f"✓ Segment final ajouté: {current_pos} → {total_duration}")
    else:
        print(f"✗ Segment final ignoré")
    
    print(f"\nRésultat final: {len(keeps)} segments à garder")
    for i, keep in enumerate(keeps):
        duration = keep['end'] - keep['start']
        print(f"Segment {i}: {keep['start']} → {keep['end']} (durée: {duration}s)")
    print("===================")
    
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
                start_val = float(segment['start'])
                end_val = float(segment['end'])
                
                print(f"Segment brut: start={start_val}, end={end_val}")
                
                # CORRECTION : Tous les timecodes du JSON Claude sont en millisecondes
                # Conversion systématique ms → secondes
                start_sec = start_val / 1000
                end_sec = end_val / 1000
                print(f"Converti ms→s: {start_val}ms→{start_sec}s, {end_val}ms→{end_sec}s")
                
                cuts_to_remove.append({"start": start_sec, "end": end_sec})
                print(f"Cut à supprimer: {start_sec}s → {end_sec}s (type: {segment.get('type', 'unknown')})")
            except (ValueError, KeyError) as e:
                print(f"Erreur conversion segment {segment}: {e}")
                continue
        
        if not cuts_to_remove:
            return {"error": "Aucun cut valide trouvé"}
        
        print(f"\n=== RÉSUMÉ CUTS À SUPPRIMER ===")
        for i, cut in enumerate(cuts_to_remove):
            duration = cut['end'] - cut['start']
            print(f"Cut {i}: {cut['start']:.3f}s → {cut['end']:.3f}s (durée: {duration:.3f}s)")
        print("================================")
        
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
            print("ERREUR: Aucun segment généré après inversion")
            
            # Debug supplémentaire
            total_cut_duration = sum(cut['end'] - cut['start'] for cut in cuts_to_remove)
            coverage_percent = (total_cut_duration / total_duration) * 100
            print(f"Durée totale des cuts: {total_cut_duration:.3f}s")
            print(f"Couverture: {coverage_percent:.1f}% du fichier")
            
            return {"error": f"Aucun segment à garder après inversion - cuts couvrent {coverage_percent:.1f}% du fichier ({total_cut_duration:.1f}s sur {total_duration:.1f}s)"}
        
        print("=== SEGMENTS À TRAITER PAR FFMPEG ===")
        for i, keep in enumerate(processed_segments):
            duration = keep['end'] - keep['start']
            print(f"Segment FFMPEG {i}: {keep['start']:.3f}s → {keep['end']:.3f}s (durée: {duration:.3f}s)")
        print("=====================================")
        
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
            print(f"Découpe simple: {segment['start']} → {segment['end']}")
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
                valid_segment_count = 0
                
                for i, segment in enumerate(processed_segments):
                    start = segment['start']
                    duration = segment['end'] - segment['start']
                    print(f"Préparation segment {i}: start={start}, duration={duration}")
                    if duration < 0.1:  # Ignorer segments trop courts
                        print(f"Segment {i} ignoré (trop court): {duration}s")
                        continue
                    filter_parts.append(f"[0:v]trim=start={start}:duration={duration},setpts=PTS-STARTPTS[v{valid_segment_count}]")
                    filter_parts.append(f"[0:a]atrim=start={start}:duration={duration},asetpts=PTS-STARTPTS[a{valid_segment_count}]")
                    valid_segment_count += 1
                
                if not filter_parts:
                    return {"error": "Tous les segments sont trop courts après filtrage"}
                
                print(f"Segments valides pour FFMPEG: {valid_segment_count}")
                
                if valid_segment_count == 1:
                    # Un seul segment valide, pas de concat nécessaire
                    full_filter = ';'.join(filter_parts)
                    cmd = [
                        'ffmpeg', '-i', input_path,
                        '-filter_complex', full_filter,
                        '-map', '[v0]', '-map', '[a0]',
                        '-c:v', 'libx264', '-c:a', 'aac',
                        '-y', output_path
                    ]
                else:
                    # Concaténer tous les segments
                    video_inputs = ''.join([f"[v{i}]" for i in range(valid_segment_count)])
                    audio_inputs = ''.join([f"[a{i}]" for i in range(valid_segment_count)])
                    concat_filter = f"{video_inputs}concat=n={valid_segment_count}:v=1:a=0[outv]"
                    concat_filter += f";{audio_inputs}concat=n={valid_segment_count}:v=0:a=1[outa]"
                    
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
                valid_segment_count = 0
                
                for i, segment in enumerate(processed_segments):
                    start = segment['start']
                    duration = segment['end'] - segment['start']
                    if duration < 0.1:
                        continue
                    filter_parts.append(f"[0:a]atrim=start={start}:duration={duration},asetpts=PTS-STARTPTS[a{valid_segment_count}]")
                    valid_segment_count += 1
                
                if not filter_parts:
                    return {"error": "Tous les segments audio sont trop courts"}
                
                if valid_segment_count == 1:
                    full_filter = ';'.join(filter_parts)
                    cmd = [
                        'ffmpeg', '-i', input_path,
                        '-filter_complex', full_filter,
                        '-map', '[a0]',
                        '-c:a', 'aac', '-y', output_path
                    ]
                else:
                    audio_inputs = ''.join([f"[a{i}]" for i in range(valid_segment_count)])
                    concat_filter = f"{audio_inputs}concat=n={valid_segment_count}:v=0:a=1[outa]"
                    
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
                valid_segment_count = 0
                
                for i, segment in enumerate(processed_segments):
                    start = segment['start']
                    duration = segment['end'] - segment['start']
                    if duration < 0.1:
                        continue
                    filter_parts.append(f"[0:v]trim=start={start}:duration={duration},setpts=PTS-STARTPTS[v{valid_segment_count}]")
                    valid_segment_count += 1
                
                if not filter_parts:
                    return {"error": "Tous les segments vidéo sont trop courts"}
                
                if valid_segment_count == 1:
                    full_filter = ';'.join(filter_parts)
                    cmd = [
                        'ffmpeg', '-i', input_path,
                        '-filter_complex', full_filter,
                        '-map', '[v0]',
                        '-c:v', 'libx264', '-y', output_path
                    ]
                else:
                    video_inputs = ''.join([f"[v{i}]" for i in range(valid_segment_count)])
                    concat_filter = f"{video_inputs}concat=n={valid_segment_count}:v=1:a=0[outv]"
                    
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

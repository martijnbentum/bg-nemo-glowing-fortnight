import json
from pathlib import Path
import load
import locations

def load_subset_5000_clean_segments():
    segments = load.load_subset_5000_clean_segments()
    return segments

def make_nemo_manifest(output_filename = None, segments = None, 
    tar_base_path = None, audio_extension = '.ogg', dataset_id = 7, 
    overwrite = False):
    if output_filename is not None:
        p = Path(output_filename)
        if p.exists() and not overwrite:
            print(f"File {p} exists, not overwriting.")
            return
    d = []
    for segment in segments:
        nm = segment_to_nemo_format(segment, tar_base_path, 
            audio_extension, dataset_id)
        d.append(nm)
    if output_filename is not None:
        with open(p, 'w') as f:
            for nm in d:
                json_line = json.dumps(nm)
                f.write(json_line + '\n')
    return d


def segment_to_nemo_format(segment, tar_base_path = None, 
    audio_extension = '.ogg', dataset_id = 7):
    if tar_base_path is None:
        tar_base_path = locations.tar_base_path
    else: tar_base_path = Path(tar_base_path)
    tar_base_path = tar_base_path / segment['org_split']
    af = segment['audio_filepath'].replace('.wav', audio_extension)
    tf = segment['tar_filename'].replace('.tar', '_ogg.tar')
    tf = tar_base_path / tf
    af = f'tarred_audio_file:{tf}::{af}'
    names = ['name', 'identifier', 'segment_id']
    identifier = '-'.join([segment[name] for name in names])
    d = {}
    d['audio_filepath'] = af
    d['duration'] = segment['duration']
    d['text'] = segment['text']
    d['id'] = identifier
    return d


import json
from pathlib import Path
from progressbar import progressbar
import load
import locations

def load_subset_5000_clean_segments():
    segments = load.load_subset_5000_clean_segments()
    return segments

def make_nemo_manifest(output_filename = None, segments = None, 
    audio_base_path = None, audio_extension = '.ogg', dataset_id = 7, 
    overwrite = False):
    if output_filename is not None:
        p = Path(output_filename)
        if p.exists() and not overwrite:
            print(f"File {p} exists, not overwriting.")
            return
    d = []
    print(f'handling {len(segments)} segments')
    for segment in progressbar(segments):
        nm = segment_to_nemo_format(segment, audio_base_path, 
            audio_extension, dataset_id)
        d.append(nm)
    if output_filename is not None:
        with open(p, 'w') as f:
            for nm in d:
                json_line = json.dumps(nm)
                f.write(json_line + '\n')
    return d

def segment_to_nemo_format(segment, audio_base_path = None, 
    audio_extension = '.ogg', dataset_id = 7):
    if audio_base_path is None:
        audio_base_path = locations.audio_base_path
    else: audio_base_path = Path(audio_base_path)
    tf = Path(segment['tar_filename']).stem  + '_ogg'
    audio_base_path = audio_base_path / tf
    af = segment['audio_filepath'].replace('.wav', audio_extension)
    af = audio_base_path / af
    names = ['name', 'identifier', 'segment_id']
    identifier = '-'.join([segment[name] for name in names])
    d = {}
    d['audio_filepath'] = str(af)
    d['duration'] = segment['duration']
    d['text'] = segment['text']
    d['id'] = identifier
    d['dataset'] = dataset_id
    return d

def segment_to_tar_audio_file_dict(segment, output_dict = {}, 
    tar_base_path = None):
    if tar_base_path is None:
        tar_base_path = locations.tar_base_path
    else: tar_base_path = Path(tar_base_path)
    tar_filename = Path(segment['tar_filename'])
    tar_filename = f'{tar_filename.stem}_ogg.tar'
    split = segment['org_split']
    tar_filename = str(tar_base_path / split / tar_filename)
    audio_filepath = segment['audio_filepath'].replace('.wav', '.ogg')
    if tar_filename not in output_dict:
        output_dict[tar_filename] = []
    output_dict[tar_filename].append(audio_filepath)
    return output_dict

def make_tar_audio_file_dict(segments, output_filename = None, 
        tar_base_path = None, overwrite = False):
    if output_filename == None:
        output_filename = locations.BASE_DIR / 'tar_audio_file_dict_5000h.json'
    p = Path(output_filename)
    if p.exists() and not overwrite:
        print(f'Loading existing tar audio file dict from {p}')
        with open(p) as fin:
            output_dict = json.load(fin)
    print(f'Creating tar audio file dict at {p}')
    output_dict = {}
    for segment in progressbar(segments):
        output_dict = segment_to_tar_audio_file_dict(segment, output_dict,
            tar_base_path)
    with open(p, 'w') as fout:
        json.dump(output_dict, fout, indent=2)
    return output_dict


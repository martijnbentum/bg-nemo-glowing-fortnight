import json
import locations
from pathlib import Path
from progressbar import progressbar

def load_manifest(filename):
    with open(filename) as f:
        t = f.read().split('\n')
    output = []
    error = []
    for line in t:
        try: d = json.loads(line)
        except: error.append(line); continue
        info = audio_filepath_to_info(d['audio_filepath'])
        d.update(info)
        output.append(d)
    split = filename.parent.name
    tar_filename = filename.stem.replace('.manifest', '.tar')
    return {'manifest_filename': filename.name, 'split': split, 'data': output, 
        'error': error, 'tar_filename': tar_filename}

def load_all_manifests(filenames = None):
    if filenames is None:
        filenames = locations.manifest_filenames
    manifests = []
    for f in progressbar(filenames):
        manifests.append(load_manifest(f))
    return manifests

def load_test_manifests():
    return load_all_manifests(locations.test_filenames)

def load_validation_manifests():
    return load_all_manifests(locations.validations_filenames)

def load_train_manifests():
    return load_all_manifests(locations.train_filenames)

def audio_filepath_to_info(filename):
    filename = Path(filename)
    info = {'audio_filepath': str(filename)}

    o = filename.stem.split('-')
    if len(o) == 3:
        name, identifier, segment_id = o
    elif len(o) == 2:
        identifier, segment_id = o
        name = None
    else:
        m = f"Filename {filename} does not conform to expected format:"
        m += " 'name-identifier-segment_id.ext'"
        m += " or 'identifier-segment_id.ext'"
        raise ValueError(m)
    info['name'] = name
    info['identifier'] = identifier
    info['segment_id'] = segment_id
    return info

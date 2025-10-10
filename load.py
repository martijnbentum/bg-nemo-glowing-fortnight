import json
import locations
import pandas as pd
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

def load_csv(filename):
    df = pd.read_csv(filename)
    header = list(df.columns)
    header.append('identifier')
    data = df.values.tolist()
    for line in data:
        identifier = line[0].split('.')[0].split('-')[-1]
        line.append(identifier)
    return header, data

def _make_csv_episode_id_dict(filename):
    header, data = load_csv(filename)
    episode_id_dict = {}
    for line in data:
        identifier = line[-1]
        if identifier not in episode_id_dict:
            episode_id_dict[identifier] = []
        episode_id_dict[identifier].append(line) 
    return episode_id_dict

def make_or_load_episode_dict(overwrite=False):
    if locations.episode_dict_filename.exists() and not overwrite:
        print(f'Loading episode dict from {locations.episode_dict_filename}')
        with open(locations.episode_dict_filename) as f:
            d = episode_id_dict = json.load(f)
        return d
    print('Creating episode dict from CSV segment files.')
    episode_id_dict = {}
    filenames = [locations.csv_test, locations.csv_validation, 
        locations.csv_train]
    for filename in filenames:
        d = _make_csv_episode_id_dict(filename)
        episode_id_dict.update(d)
    print(f'Saving episode dict to {locations.episode_dict_filename}')
    with open(locations.episode_dict_filename, 'w') as f:
        json.dump(episode_id_dict, f)
    return episode_id_dict
            
def load_program(name, other_programs = None):
    filename = locations.program_directory / f"{name}.json"
    if other_programs is not None and name in other_programs:
        return other_programs[name]
    if not filename.exists():
        m = f"Program file {filename} does not exist."
        with open(locations.program_directory / "other_programs.json") as f:
            d = json.load(f)
        if name in d:
            m += f" Found in other_programs.json."
            print(m)
            return d[name]
    with open(filename) as f:
        d = json.load(f)
    return d

def _load_other_programs():
    with open(locations.program_directory / "other_programs.json") as f:
        d = json.load(f)
    return d
    
def load_program_names():
    with open('../program_names.txt') as f:
        t = f.read().split('\n')
    return t

def load_program_episode_count():
    with open('../program_episode_count.txt') as f:
        t = [x.split('\t') for x in f.read().split('\n')]
    for line in t:
        line[1] = int(line[1])
    return t

def load_program_info():
    with open(locations.program_info_filename) as f:
        d = json.load(f)
    return d



import json
import locations
import pandas as pd
from pathlib import Path
from progressbar import progressbar

NO_NAME = '~~~no_name~~~'

def load_manifest(filename):
    '''manifest json file contains a json object per line
    the manifest correspond to a single tar file with audio files
    '''
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
    '''load all manifests from a list of filenames
    default is all manifests in locations.manifest_filenames
    '''
    if filenames is None:
        filenames = locations.manifest_filenames
    manifests = []
    for f in progressbar(filenames):
        manifests.append(load_manifest(f))
    return manifests

def load_test_manifests():
    '''load all manifest files from the test set'''
    return load_all_manifests(locations.test_filenames)

def load_validation_manifests():
    '''load all manifest files from the validation set'''
    return load_all_manifests(locations.validations_filenames)

def load_train_manifests():
    '''load all manifest files from the train set'''
    return load_all_manifests(locations.train_filenames)

def audio_filepath_to_info(filename):
    '''Extract name, identifier, and segment_id from audio filename.
    name is the program name, which may be absent
    identifier is the episode identifier (this is not unique accross programs)
    segment_id is the segment identifier within the episode
        this is an integer value that starts at 0 for each episode
    '''
    filename = Path(filename)
    info = {'audio_filepath': str(filename)}

    o = filename.stem.split('-')
    if len(o) == 3:
        name, identifier, segment_id = o
    elif len(o) == 2:
        identifier, segment_id = o
        name = NO_NAME
    else:
        m = f"Filename {filename} does not conform to expected format:"
        m += " 'name-identifier-segment_id.ext'"
        m += " or 'identifier-segment_id.ext'"
        raise ValueError(m)
    info['name'] = name
    info['identifier'] = identifier
    info['segment_id'] = segment_id
    return info

# csv whisper metadata files

def load_csv(filename):
    '''csv files are whisper metadata files for test, validation, train

    they contain the original whisper transcriptions, start and end
    samples and the episode audio filename (which is program-identifier)
    difference between whisper transcription and transcription in manifest is 
    probably only that the manifest transcription is lower cased.

    some transcriptions seem to contain hallucinations.
    '''
    df = pd.read_csv(filename)
    header = list(df.columns)
    header.extend(['name','identifier'])
    data = df.values.tolist()
    for line in data:
        stem = line[0].split('/')[-1].split('.')[0]
        try: name, identifier = stem.split('-')
        except: 
            print(f'Filename {stem} does not conform to expected format')
            identifier = stem.split('-')[-1]
            name = NO_NAME
        line.extend([name,identifier])
    return header, data

def _make_csv_episode_id_dict(filename, episode_id_dict = {}):
    '''make episode id dict from a single csv file (test, validation, train)
    '''
    header, data = load_csv(filename)
    print(f'handling {filename} with {len(data)} rows')
    for line in progressbar(data):
        identifier = '-'.join(line[-2:])
        if identifier not in episode_id_dict:
            episode_id_dict[identifier] = []
        episode_id_dict[identifier].append(line) 
    return episode_id_dict

def _make_or_load_csv_program_episode_dict(overwrite=False):
    '''make or load a dict of program  episode ids for all splits 
    (test, validation, train)
    since episode ids are not unique accross programs
    the keys are 'program-episode_id'
    the values are the list of csv rows for that episode ie segments
    '''
    if locations.csv_episode_dict_filename.exists() and not overwrite:
        p = locations.csv_episode_dict_filename
        print(f'Loading episode dict from {p}')
        with open(locations.csv_episode_dict_filename) as f:
            d = episode_id_dict = json.load(f)
        return d
    print('Creating episode dict from CSV segment files.')
    episode_id_dict = {}
    filenames = [locations.csv_test, locations.csv_validation, 
        locations.csv_train]
    for filename in filenames:
        _make_csv_episode_id_dict(filename, episode_id_dict)
    print(f'Saving episode dict to {locations.csv_episode_dict_filename}')
    with open(locations.csv_episode_dict_filename, 'w') as f:
        json.dump(episode_id_dict, f)
    return episode_id_dict
            
def load_csv_program(name, other_programs = None):
    '''load a program json file by name from locations.csv_program_directory
    the programs are based on the csv files
    a program contains episodes, which contain segments

    all programs with a single episode are stored in other_programs.json
    '''
    filename = locations.csv_program_directory / f"{name}.json"
    if other_programs is not None and name in other_programs:
        return other_programs[name]
    if not filename.exists():
        m = f"Program file {filename} does not exist."
        with open(locations.csv_program_directory / "other_programs.json") as f:
            d = json.load(f)
        if name in d:
            m += f" Found in other_programs.json."
            print(m)
            return d[name]
    with open(filename) as f:
        d = json.load(f)
    return d

def _load_other_csv_programs():
    '''all programs with a single episode are stored in other_programs.json
    '''
    with open(locations.csv_program_directory / "other_programs.json") as f:
        d = json.load(f)
    return d
    
def load_program_names():
    '''return a list of all program names from program_names.txt'''
    with open('../program_names.txt') as f:
        t = f.read().split('\n')
    t = [x for x in t if x]
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


def load_csv_episode_segments(program_name, episode_id, other_programs = None):
    '''load the csv segments for a given program name and episode id
    other_programs is a dict of programs with a single episode
    if not provided it will be loaded from other_programs.json which slows down
    processing
    '''
    program = load_csv_program(program_name, other_programs)
    if episode_id not in program['episodes']:
        m = f"Episode id {episode_id} not found in program {program_name}."
        raise ValueError(m)
    csv_segments = program['episodes'][episode_id]
    return csv_segments

def load_hallucination_program(name):
    '''load hallucination metrics for a given program name'''
    f = locations.hallucinations_directory / f'{name}_hallucinations.json'
    if not f.exists():
        m = f"Hallucination file {f} does not exist."
        raise ValueError(m)
    with open(f) as fin:
        d = json.load(fin)
    return d

def _load_other_manifest_programs():
    '''all programs with a single episode are stored in other_programs.json
    '''
    with open(locations.manifest_program_directory / "other_programs.json") as f:
        d = json.load(f)
    return d

def load_manifest_program(name, other_programs = None):
    '''load manifest for a given program name'''
    f = locations.manifest_program_directory / f'{name}.json'
    if other_programs is not None and name in other_programs:
        return other_programs[name]
    if not f.exists():
        m = f"Manifest program file {f} does not exist."
        raise ValueError(m)
    with open(f) as fin:
        d = json.load(fin)
    return d

def load_manifest_program_dict(other_programs = None):
    if other_programs is None:
        other_programs = _load_other_manifest_programs()
    pn = load_program_names()
    d = {}
    for name in progressbar(pn):
        d[name] = load_manifest_program(name, other_programs)
    return d

def load_program(name):
    f = locations.program_directory / f'{name}.json'
    if not f.exists():
        m = f"program file {f} does not exist."
        raise ValueError(m)
    with open(f) as fin:
        d = json.load(fin)
    return d

def load_program_dict():
    d = {}
    pn = load_program_names()
    for name in progressbar(pn):
        d[name] = load_program(name)
    return d


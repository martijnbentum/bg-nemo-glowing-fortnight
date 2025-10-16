from collections import Counter
import json
import load
import locations
from progressbar import progressbar

'''preprocessing module'''

locations.program_directory.mkdir(exist_ok=True)

def _find_episode_indices(name, names):
    return [i for i, x in enumerate(names) if x == name]

def csv_episode_dict_to_names(d):
    names = [k.split('-')[0] for k in d.keys()]
    return names

def csv_episode_dict_to_episode_ids(d):
    episode_ids = [k.split('-')[-1] for k in d.keys()]
    return episode_ids

def make_program_files(d = None, names = None):
    '''make program json files from the csv episode dict
    the program files are stored in locations.program_directory
    they are based on the csv files with whisper metadata
    '''
    if d is None:
        d = load.make_or_load_csv_episode_dict()
    if names is None:
        names = csv_episode_dict_to_names(d)
    episode_ids = csv_episode_dict_to_episode_ids(d)
    program_names = list(set(names))
    other_programs = {}
    for name in progressbar(program_names):
        indices = _find_episode_indices(name, names)
        program_episode_ids = [episode_ids[i] for i in indices]
        if len(program_episode_ids) == 1:
            program = handle_program(name, program_episode_ids, d, save=False)
            other_programs[name] = program
        else:
            handle_program(name, program_episode_ids, d, save=True)
    other_filename = locations.program_directory / "other_programs.json"
    m = f"Writing other programs file {other_filename} with "
    m += f"{len(other_programs)} programs."
    print(m)
    with open(other_filename, 'w') as f:
        json.dump(other_programs, f)


def handle_program(name, program_episode_ids, d, save = True):
    '''make a single program json file with all associated episodes
    '''
    filename = locations.program_directory / f"{name}.json"
    episodes = {eid: d[name+'-'+eid] for eid in program_episode_ids}
    duration = 0
    n_segments = 0
    for episode in episodes.values():
        for segment in episode:
            duration += segment[-3]
        n_segments += len(episode)
    n_episodes = len(episodes)
    program_dict = {
        'name': name,
        'n_episodes': n_episodes,
        'n_segments': n_segments,
        'total_duration': duration,
        'episodes': episodes
    }
     
    if save:
        m = f"Writing program file {filename} with {len(program_episode_ids)}."
        m += f" episodes, {n_segments} segments, {duration/3600:.2f} hours."
        print(m)
        with open(filename, 'w') as f:
            json.dump(program_dict, f)
    return program_dict

def _make_program_info(overwrite = False):
    '''make a single json file with summary info for all programs
    the information is the same as in the program files except the episodes
    are removed and total duration is in hours
    '''
    if locations.program_info_filename.exists() and not overwrite:
        print(f"Program info file {locations.program_info_filename} exists.")
        return
    names = load_program_names()
    output = {}
    other_programs = load._load_other_programs()
    for name in progressbar(names):
        program = load_program(name, other_programs)
        if program is None:
            program = other_programs[name]
        if 'episodes' not in program:
            print(f"Program {name} has no episodes.")
            continue
        del program['episodes']
        program['total_duration_hours'] = program['total_duration'] / 3600
        del program['total_duration']
        output[name] = program
    with open(locations.program_info_filename, 'w') as f:
        json.dump(output, f, indent=4)
        
    
def load_program(name, other_programs = None):
    return load.load_program(name, other_programs = other_programs)

def load_program_names():
    return load.load_program_names()
        
def load_program_info():
    return load.load_program_info()

        
    

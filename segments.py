import json
import load
import locations
from progressbar import progressbar
import random


def load_program_dict():
    return load.load_program_dict()

def filter_segments(program_dict = None, save = False):
    if program_dict is None:
        program_dict = load_program_dict()
    filtered = {}
    no_text = []
    hallucinations = []
    clean = []
    for program_name, program in progressbar(program_dict.items()):
        for episode_id, episode in program['episodes'].items():
            for segment in episode['segments']:
                if segment['ngram_ratio'] is None:
                    no_text.append(segment)
                elif segment['ngram_ratio'] > 0.3:
                    hallucinations.append(segment)
                else: clean.append(segment)
    if save:
        save_segments_list(clean, 'segments.json', overwrite = True)
        save_segments_list(hallucinations, 'hallucination_segments.json', 
            overwrite = True)
        save_segments_list(no_text, 'no_text_segments.json', overwrite = True)
    return clean, hallucinations, no_text

def segment_list_to_duration(segments):
    duration = []
    for segment in segments:
        duration.append( segment['duration'] )
    return duration

def save_segments_list(segments, filename, overwrite = False):
    p = locations.BASE_DIR / filename
    if p.exists() and not overwrite:
        print(f"File {p} exists, not overwriting.")
        return
    with open(p, 'w') as f:
        for segment in segments:
            json_line = json.dumps(segment)
            f.write(json_line + '\n')

def load_clean_segments():
    return load.load_clean_segments()

def shuffle_and_subset(segments = None, duration_hours = 5000, seed = 42):
    random.seed(seed)
    if segments is None:
        segments = load_clean_segments()
    duration_seconds = duration_hours * 3600
    durs = segment_list_to_duration(segments)
    total_dur = sum(durs)
    if total_dur <= duration_seconds:
        m = f'Total duration {total_dur/3600:.2f} hours is less than '
        m += f'requested {duration_hours} hours.'
        raise ValueError(m)
    m = f'Total duration {total_dur/3600:.2f} hours, # segments {len(segments)}.'
    print(m)
    print('shuffling and subsetting ...')
    random.shuffle(segments)
    subset = []
    selected_duration = 0
    for segment in progressbar(segments):
        selected_duration += segment['duration']
        if selected_duration > duration_seconds:
            break
        subset.append(segment)
    m = f' After subsetting, total duration {selected_duration/3600:.2f} '
    m += f'hours, # segments {len(subset)}.'
    print(m)
    return subset

def subset_5000_clean(overwrite = False):
    f = locations.BASE_DIR / 'clean_5000h_segments.json'
    if f.exists() and not overwrite:
        print(f'loading {f} ...')
        return load.load_subset_5000_clean_segments()
    print(f'creating {f} ...')
    segments = shuffle_and_subset(duration_hours = 5000)
    save_segments_list(segments, f.name, 
        overwrite = overwrite)
    return segments
        
        


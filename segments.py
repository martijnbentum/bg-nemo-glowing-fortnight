import json
import load
import locations
from progressbar import progressbar

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

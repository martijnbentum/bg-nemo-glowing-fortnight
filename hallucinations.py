import copy
import json
import load
import locations
from progressbar import progressbar
import re

'''preprocessing module'''


def text_to_word(text):
    words = re.findall(r"\w+", text)
    return words

def compute_type_token_ratio(text):
    words = text_to_word(text)
    types = list(set(words))
    type_token_ratio = len(types) / len(words)
    print(f"Types: {len(types)}, Tokens: {len(words)})")
    print(f" TTR: {type_token_ratio:.3f}")
    return type_token_ratio

def detect_repeated_phrases(text, n=3):
    ''' Detects repeated n-grams (of n words) anywhere in the text.
    Returns a dict {ngram: count} for repeats.
    '''
    words = re.findall(r'\w+', text)
    ngrams = [' '.join(words[i:i+n]) for i in range(len(words)-n+1)]

    counts = {}
    for g in ngrams:
        counts[g] = counts.get(g, 0) + 1

    return {k: v for k, v in counts.items() if v > 1}


def ngram_repeat_coverage(text,n=5):
    '''Compute coverage of text by repeated n-grams.
    text - input text string
    n - n-gram size (default 5)
    '''
    words = text_to_word(text)
    total_tokens = len(words)
    if total_tokens == 0: return None
    covered = [False] * total_tokens
    positions = {}

    # collect all n-gram positions
    for i in range(total_tokens - n + 1):
        ngram = " ".join(words[i:i+n])
        if ngram not in positions: positions[ngram] = []
        positions[ngram].append(i)

    # keep only repeats
    repeats= {ngram: ids for ngram, ids in positions.items() if len(ids) > 1}

    # mark coverage
    for ngram, start_indices in repeats.items():
        for i in start_indices:
            for t in range(i, i + n):
                covered[t] = True

    repeats = {k: len(v) for k,v in repeats.items()}

    covered_tokens = sum(covered)
    coverage_ratio = covered_tokens / total_tokens
    output = {"coverage_ratio": coverage_ratio,
        "covered_tokens": covered_tokens,"total_tokens": total_tokens,
        "repeats": repeats,"total_repeats": sum(repeats.values()) }
    return output

def csv_segment_to_text(segment):
    return segment[3].strip()

def csv_episode_to_texts(episode):
    texts = [csv_segment_to_text(s) for s in episode]
    return texts

def csv_program_to_texts(program):
    texts = []
    for episode in program['episodes'].values():
        texts.extend(csv_episode_to_texts(episode))
    return texts

def csv_program_to_segment_names(program):
    segment_names= []
    for episode in program['episodes'].values():
        for segment in episode:
            segment_names.append('-'.join(segment[-2:]))
    return segment_names

def csv_program_to_segment_name_text(program):
    texts = csv_program_to_texts(program)
    names = csv_program_to_segment_names(program)
    segment_name_text= []
    for name, text in zip(names, texts):
        segment_name_text.append([name, text])
    return segment_name_text

def program_name_to_hallucination_dict_fileame(name):
    f = locations.hallucinations_directory / f'{name}_hallucinations.json'
    return f
    
def handle_program_name(name, overwrite = False, other_programs = None):
    '''Process a single program name to compute hallucination metrics.
    '''
    f = program_name_to_hallucination_dict_fileame(name)
    if f.exists() and not overwrite:
        print(f"{f} exists, skipping.")
        return
    program = load.load_csv_program(name, other_programs=other_programs)
    nt = csv_program_to_segment_name_text(program)
    output = {}
    for segment_name, text in nt:
        name, identifier = segment_name.split('-')
        if identifier not in output: output[identifier] = []
        segment_d = {'name': name, 'identifier': identifier, 'text': text}
        rp = ngram_repeat_coverage(text)
        if rp == None: 
            output[identifier].append(segment_d)
            continue
        del rp['repeats']
        segment_d.update(rp)
        output[identifier].append(segment_d)
    with open(f, 'w') as fout:
        json.dump(output, fout)
    return output

def handle_all_programs(overwrite = False, other_programs = None):
    '''Process all programs to compute hallucination metrics.
    '''
    if other_programs is None:
        other_programs = load._load_other_csv_programs()
    program_names = load.load_program_names()
    for name in progressbar(program_names):
        handle_program_name(name, overwrite, other_programs)

def load_hallucination_program(name):
    return load.load_hallucination_program(name)

def hallucination_program_to_segments(program):
    segments = []
    for episode in program.values():
        segments.extend(episode)
    return segments
    
def filter_segments(program, max_coverage=0.3):
    output = {}
    for identifier, segments in program.items():
        output[identifier] = {}
        clean= []
        hallucinations = []
        for i,segment in enumerate(segments):
            segment['segment_index'] = i
            if 'coverage_ratio' not in segment: continue
            if segment['coverage_ratio'] > max_coverage: 
                hallucinations.append(segment)
            else: clean.append(segment)
        output[identifier]['clean_segments'] = clean
        output[identifier]['hallucinations'] = hallucinations
    return output

def program_name_to_filtered_segments(name, max_coverage=0.3):
    program = load_hallucination_program(name)
    segments = hallucination_program_to_segments(program)
    clean, hallucinations = filter_segments(segments, max_coverage)
    return clean, hallucinations

def handle_manifest_programs(manifest_program_dict = None,
    other_programs = None, overwrite = False):
    if manifest_program_dict is None:
        manifest_program_dict = load.load_manifest_program_dict(other_programs)
    directory = locations.program_directory
    for name, program in progressbar(manifest_program_dict.items()):
        filename = locations.program_directory / f'{name}.json'
        if filename.exists() and not overwrite: 
            print(f"{filename} exists, skipping.")
            continue
        for identifier, episode in program['episodes'].items():
            segments = []
            for s in episode['segments']:
                s = clean_manifest_segment(s)
                o = ngram_repeat_coverage(s['text'])
                if o is not None:
                    s['ngram_ratio'] = o['coverage_ratio']
                else:
                    s['ngram_ratio'] = None
                    print(f'no text in segment {s}')
                segments.append(s)
            episode['segments'] = segments
        with open(filename, 'w') as fout:
            json.dump(program, fout)
    return manifest_program_dict

                
                
def clean_manifest_segment(segment):
    s = copy.copy(segment)
    s['text'] = s['whisper_text']
    s['org_start'] = s['start_time']
    s['org_end'] = s['end_time']
    s['org_split'] = s['split']
    del s['split']
    del s['start_time']
    del s['end_time']
    del s['whisper_text']
    del s['manifest_filename']
    del s['levenshtein_ratio']
    return s
    
    

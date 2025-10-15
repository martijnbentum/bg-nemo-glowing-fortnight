import json
import Levenshtein
import load
import locations
from progressbar import progressbar
import segment_matcher
import textwrap

def load_all_manifests(filenames = None):
    '''Load all manifest files corresponding to tarred sets of audio files.
    '''
    return load.load_all_manifests(filenames)

class Episode:
    def __init__(self, identifier, segments, parent = None, do_match = True):
        self.identifier = identifier
        self.segments = segments
        self.parent = parent
        self.name = segments[0].name if segments else None
        for segment in segments:
            segment.episode = self
        self._handle_match()

    def __repr__(self):
        m = f"<Episode {self.name} {self.identifier} "
        m += f"({len(self.segments)} segments)>"
        return m

    def to_json(self):
        return {
            'name': self.name,
            'identifier': self.identifier,
            'total_duration': sum(s.duration for s in self.segments),
            'segments': [s.to_json() for s in self.segments]
        }

    def csv_segments(self):
        other_programs = self.parent.other_programs if self.parent else None
        csv_segments = load.load_csv_episode_segments(
            self.name, self.identifier, other_programs)
        return csv_segments

    def _handle_match(self):
        try: segment_matcher.match_episode_segments(
            self.segments, self.csv_segments(), self.identifier)
        except Exception as e:
            m = f"Error matching segments for episode {self.identifier}: {e}"
            print(m)
            self.match_ok = False
        else: 
            self.match_ok = True
            for s in self.segments:
                s.levenshtein_ratio = Levenshtein.ratio(s.text.lower(), 
                    s.whisper_text.lower())
                

class Segments:
    def __init__(self, manifest_filenames = None, other_programs = None, 
        do_match = True, make_episodes = False):
        if manifest_filenames is None:
            manifest_filenames = locations.manifest_filenames
        if other_programs is None:
            self.other_programs = load._load_other_programs()
        else: self.other_programs = other_programs
        if not make_episodes: do_match = False
        self.do_match = do_match
        print('make segments')
        self._handle_make_segments(manifest_filenames)
        if make_episodes:
            print('make episodes')
            self._handle_make_episodes()
        self.match_handled = do_match

    def __repr__(self):
        return f"<Segments {len(self.segments)} segments>"

    def _handle_make_segments(self, manifest_filenames):
        manifests = load_all_manifests(manifest_filenames)
        self.segments = []
        for item in progressbar(manifests):
            tar_filename = item['tar_filename']
            split = item['split']
            for line in item['data']:
                line['tar_filename'] = item['tar_filename']
                line['manifest_filename'] = item['manifest_filename']
                line['split'] = item['split']
                segment = Segment(**line)
                self.segments.append(segment)
        self.segments.sort(key = lambda x: (x.name, x.identifier, int(x.segment_id)))

    def _handle_make_episodes(self):
        self.episodes = []
        identifier = self.segments[0].identifier
        segments = []
        for segment in progressbar(self.segments):
            if segment.identifier != identifier:
                self.episodes.append(Episode(identifier, segments, self,
                    do_match = self.do_match))
                segments = []
                identifier = segment.identifier
            segments.append(segment)
        if segments:
            self.episodes.append(Episode(identifier, segments, self,
                do_match = self.do_match))
        self.episodes.sort(key = lambda x: (x.name, x.identifier))


    def to_segment_json(self):
        return [segment.to_json() for segment in self.segments]

    def handle_match(self):
        if self.match_handled: 
            print("Match already handled.")
            return
        for episode in progressbar(self.episodes):
            episode._handle_match()
        self.match_handled = True

class Segment:
    def __init__(self, text, audio_filepath, name, identifier, segment_id,
        tar_filename, manifest_filename, split, duration):
        self.text = text
        self.audio_filepath = audio_filepath
        self.duration = duration
        self.name = name
        self.identifier = identifier
        self.segment_id = segment_id
        self.tar_filename= tar_filename
        self.manifest_filename = manifest_filename
        self.split = split
        self.episode = None
        self.program = None

    def __repr__(self):
        m = f"<Segment {self.name} {self.identifier} {int(self.segment_id):>4} "
        m += f" {self.duration:>5.2f} s  ({self.split})"
        return m

    def __str__(self):
        m = self.__repr__() + '\n'
        items = self.__dict__.items()
        o = []
        for k,v in self.__dict__.items():
            if k in ['episode', 'program']: continue
            if k in ['text', 'whisper_text'] and v is not None: 
                v = textwrap.fill(v, width=80, subsequent_indent=' ' * 20)
            o.append(f'{k:<18}: {v}')
        m += '\n'.join(o)
        return m
        

    def __gt__(self, other):
        if not isinstance(other, Segment):
            raise ValueError("Can only compare Segment to Segment")
        if self.identifier != other.identifier:
            raise ValueError("Can only compare Segment with same identifier")
        return self.segment_id > other.segment_id

    def to_json(self):
        d = {}
        keys = ['audio_filepath', 'text', 'duration', 'name', 'identifier',
            'segment_id', 'tar_filename', 'manifest_filename', 'split']
        optional_keys = ['whisper_text', 'start_time', 'end_time', 
            'levenshtein_ratio']
        for k in keys:
            d[k] = getattr(self, k)
        for k in optional_keys:
            if hasattr(self, k):
                d[k] = getattr(self, k)
        return d

def identifier_to_episode(identifier, segments, do_match = True, 
    segments_object = None):
    segments = [s for s in segments if s.identifier == identifier]
    names = list(set([x.name for x in segments]))
    if len(names) > 1:
        episodes = []
        print(f'Warning: identifier {identifier} has segments from ')
        print(f'multiple names: {names}, return multiple episodes')
        for name in names:
            sub_segments = [s for s in segments if s.name == name]
            episodes.append(Episode(identifier, sub_segments, 
                segments_object, do_match = do_match))
        return episodes
    return Episode(identifier, segments, segments_object, do_match = do_match)

def identifiers_to_episode(identifiers, segments, do_match = True,
    segments_object = None):
    print('select segments with given identifiers')
    selection = []
    for segment in progressbar(segments):
        if segment.identifier in identifiers:
            selection.append(segment)
    episodes = []
    print(f'making {len(identifiers)} episodes')
    for identifier in progressbar(identifiers):
        episodes.append(identifier_to_episode(identifier, selection, do_match,
            segments_object))
    return episodes


    
def make_manifest_programs(segments):
    '''Given a Segments object, make manifest program files for each program
    this links csv segments to manifest segments and checks the levenshtein ratio
    '''
    print('extracting program names')
    names = [s.name for s in segments.segments]
    print('grouping segments by program name')
    segment_slices = program_names_to_segment_slices(names, segments.segments)
    other_programs = {}
    for segment_slice in progressbar(segment_slices):
        ids = list(set([s.identifier for s in segment_slice]))
        if len(ids) != 1: continue
        eps = identifiers_to_episode(ids, segment_slice, do_match = True,
            segments_object = segments)
        d = episodes_to_manifest_program_json(eps, save = True)
        if d['n_episodes'] == 1:
            other_programs[d['name']] = d
    other_filename = locations.manifest_program_directory / "other_programs.json"
    with open(other_filename, 'w') as f:
        json.dump(other_programs, f)

def program_names_to_segment_slices(program_names, segments):
    ids = _find_string_ranges(program_names)
    pn = list(set(program_names))
    segment_slices = []
    for name in pn:
        start, end = ids[name]
        segment_slices.append(segments[start:end])
    return segment_slices

def episodes_to_manifest_program_json(episodes, save = True):
    check_episodes(episodes)
    d = {}
    d['name'] = episodes[0].name 
    d['n_episodes'] = len(episodes)
    d['n_segments'] = sum(len(e.segments) for e in episodes)
    d['total_duration'] = sum(s.duration for e in episodes for s in e.segments)
    d['episodes'] = {e.identifier: e.to_json() for e in episodes}
    lr = [s.levenshtein_ratio for e in episodes for s in e.segments]
    d['min_levenshtein_ratio'] = min(lr) if lr else None
    if len(episodes) == 1: 
        print('only one episode, not saving manifest program file')
        return d
    if save:
        filename = locations.manifest_program_directory / f"{d['name']}.json"
        with open(filename, 'w') as f:
            json.dump(d, f)
    return d

def _find_string_ranges(sorted_strings):
    """
    Given a sorted list of strings, return a dict mapping each unique string
    to its (start_index, end_index) in the list (end is exclusive).
    """
    ranges = {}
    if not sorted_strings:
        return ranges

    start = 0
    current = sorted_strings[0]

    for i, s in enumerate(sorted_strings[1:], start=1):
        if s != current:
            ranges[current] = (start, i)
            start = i
            current = s

    # add the last group
    ranges[current] = (start, len(sorted_strings))
    return ranges

def check_episodes(episodes):
    names = list(set([e.name for e in episodes]))
    if len(names) != 1:
        m = "Episodes do not all have the same name."
        m += f" Found {names}"
        raise ValueError(m)
    for e in episodes:
        for s in e.segments:
            if s.levenshtein_ratio < .7:
                m = f"Low Levenshtein ratio {s.levenshtein_ratio:.2f} "
                m += f"for segment {s.name} {s.identifier} {s.segment_id} "
                print(m)


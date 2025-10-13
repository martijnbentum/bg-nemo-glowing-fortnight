import load
import locations
from progressbar import progressbar
import segment_matcher
import textwrap

def load_all_manifests(filenames = None):
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
            print(f"Error matching segments for episode {self.identifier}: {e}")
            self.match_ok = False
        else: self.match_ok = True

class Segments:
    def __init__(self, manifest_filenames = None, other_programs = None, 
        do_match = True):
        if manifest_filenames is None:
            manifest_filenames = locations.manifest_filenames
        if other_programs is None:
            self.other_programs = load._load_other_programs()
        else: self.other_programs = other_programs
        self.do_match = do_match
        print('make segments')
        self._handle_make_segments(manifest_filenames)
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
        m = f"<Segment {self.identifier} {int(self.segment_id):>4} "
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


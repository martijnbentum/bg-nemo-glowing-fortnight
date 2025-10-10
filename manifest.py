import load
import locations
from progressbar import progressbar
import textwrap

def load_all_manifests(filenames = None):
    return load.load_all_manifests(filenames)

class Program:
    def __init__(self, name, segments):
        self.name = name
        self.segments = segments
        self.identifiers = set(s.identifier for s in segments)
        self.episodes = []
        for identifier in self.identifiers:
            segments = [s for s in segments if s.identifier == identifier]
            segments.sort()
            episode = Episode(identifier, segments, self)
            self.episodes.append(episode)
        self.episodes.sort(key = lambda x: x.identifier)

    def __repr__(self):
        m = f"<Program {self.name} "
        m += f"({len(self.episodes)} episodes, {len(self.segments)} segments)>"
        return m

    def to_json(self):
        d = {
            'name': self.name,
            'total_duration': sum(s.duration for s in self.segments),
            'episodes': {e.identifier: e.to_json() for e in self.episodes}
        }

class Episode:
    def __init__(self, identifier, segments, program):
        self.identifier = identifier
        self.segments = segments
        self.name = segments[0].name if segments else None
        self.program = program
        for segment in segments:
            segment.episode = self
            segment.program = program

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

class Segments:
    def __init__(self, segments = None, manifest_filenames = None):
        if segments is None:
            self._handle_make_segments(manifest_filenames)
        else: self.segments = segments
        self.segments.sort(key = lambda x: (x.identifier, int(x.segment_id)))

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

    def make_programs(self):
        self.programs = []
        names = set(s.name for s in self.segments)
        for name in progressbar(names):
            segments = [s for s in self.segments if s.name == name]
            program = Program(name, segments)
            self.programs.append(program)
        self.programs.sort(key = lambda x: x.name)
    

    def to_program_json(self):
        if not hasattr(self, 'programs'):
            self.make_programs()
        d = {}
        for program in self.programs:
            d[program.name] = program.to_json()
        return d

    def to_segment_json(self):
        return [segment.to_json() for segment in self.segments]

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
        for k in keys:
            d[k] = getattr(self, k)
        return d


import load
from progressbar import progressbar

def match_episode_segments(manifest_segments, csv_segments, identifier):
    if len(manifest_segments) != len(csv_segments):
        m = "Number of segments in manifest and CSV do not match."
        m += f" manifest: {len(manifest_segments)} CSV: {len(csv_segments)}"
        m += f" for identifier {identifier}."
        raise ValueError(m)
    check_identifier(manifest_segments, csv_segments, identifier)
    for m, c in zip(manifest_segments, csv_segments):
        csv_duration = c[-2]
        if abs(m.duration - csv_duration) > 0.1:
            m = f"Segment durations do not match for identifier {identifier}."
            m += f" Manifest: {m.duration}, CSV: {csv_duration}"
            raise ValueError(m)
        m.whisper_text = c[3].strip()
        m.start_time = round(c[1] / 16000, 3)
        m.end_time = round(c[2] / 16000, 3)

def match_all_segments(manifest_segments, episode_id_dict):
    current_id = manifest_segments.segments[0].identifier
    segments = []
    for segment in progressbar(manifest_segments.segments):
        if segment.identifier != current_id:
            if current_id not in episode_id_dict:
                m = f"Identifier {current_id} not found in CSV data."
                raise ValueError(m)
            csv_segments = episode_id_dict[current_id]
            match_episode_segments(segments, csv_segments, current_id)
            segments = [segment]
            current_id = segment.identifier
        else:
            segments.append(segment)
        


def check_identifier(manifest_segments, csv_segments, identifier):
    mids = list(set([x.identifier for x in manifest_segments]))
    if len(mids) != 1: 
        m = "Manifest segments do not all have the same identifier."
        m += f" Found {mids}"
        raise ValueError(m)
    if mids[0] != identifier:
        m += f" Expected {identifier}, found {mids}"
        raise ValueError(m)
    cids = list(set([x[-1] for x in csv_segments]))
    if len(cids) != 1:
        m = "CSV segments do not all have the same identifier."
        m += f" Found {cids}"
        raise ValueError(m)
    if cids[0] != identifier:
        m += f" Expected {identifier}, found {cids}"
        raise ValueError(m)

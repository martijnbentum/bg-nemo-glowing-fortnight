import re
from collections import defaultdict

def text_to_word(text):
    words = text.split(' ')
    words = [w for w in words if w != '']
    return words

def compute_type_token_ratio(text):
    words = text_to_word(text)
    types = list(set(words))
    type_token_ratio = len(types) / len(words)
    print(f"Types: {len(types)}, Tokens: {len(words)}, TTR: {type_token_ratio:.3f}")
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


def ngram_repeat_coverage(text,n=5,include_first_occurrence=True):
    """
    Compute coverage of text by repeated n-grams.
    """
    # tokenize into "words"  
    words = re.findall(r"\w+", text)
    total_tokens = len(words)
    if total_tokens == 0:
        raise ValueError("Text contains no tokens.")

    repeats = {}  #  {ngram_str: start_positions}
    covered = [False] * total_tokens

    positions = defaultdict(list)  # ngram_str -> count

    # collect all n-gram positions
    for i in range(total_tokens - n + 1):
        ngram = " ".join(words[i:i+n])
        positions[ngram].append(i)

    # keep only repeats
    repeats_n = {ngram: idxs for ngram, idxs in positions.items() if len(idxs) > 1}

    # mark coverage
    for ngram, idxs in repeats_n.items():
        starts = idxs if include_first_occurrence else idxs[1:]
        for i in starts:
            for t in range(i, i + n):
                covered[t] = True

    repeats = {k: len(v) for k,v in positions.items()}

    covered_tokens = sum(covered)
    coverage_ratio = covered_tokens / total_tokens

    return {
        "coverage_ratio": coverage_ratio,
        "covered_tokens": covered_tokens,
        "total_tokens": total_tokens,
        "repeats": repeats,
        "positions": positions,
        "total_repeats": sum(repeats.values())
    }


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
    

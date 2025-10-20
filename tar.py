from functools import partial
from multiprocessing import Pool
from pathlib import Path
from progressbar import progressbar
import tarfile
import time
from tqdm import tqdm

def _runner(args, output_path, verbose, check_existing):
    tar_file_path, member_names = args
    return extract_files(tar_file_path, member_names, output_path, verbose,
        check_existing)

def extract_files_with_tar_audio_file_dict_mp(tar_audio_file_dict,
    output_path, num_workers = 6, verbose = False, check_existing = True):
    items = tar_audio_file_dict.items()
    print(time.ctime(), len(items), num_workers)
    with Pool(processes=num_workers) as pool:
        results = []
        for r in tqdm(
            pool.imap_unordered(
                partial(_runner, output_path = output_path, verbose = verbose,
                    check_existing = check_existing),
                items, chunksize = 1), 
                total= len(items)):
            results.append(r)
            print(r, 'done', len(results), len(items), time.ctime())

def extract_files_with_tar_audio_file_dict(tar_audio_file_dict, output_path, 
    verbose = False):
    for tar_file_path, member_names in progressbar(tar_audio_file_dict.items()):
        extract_files(tar_file_path, member_names, output_path, verbose)

def extract_files(tar_file_path, member_names, output_path, verbose = False,
    check_existing = False):
    if check_existing:
        all_exist, present_files, missing_files = check_whether_files_exist(
            tar_file_path, member_names, output_path)
        if all_exist:
            m = f'All files already exist for {tar_file_path}, skipping.'
            if verbose:print(m)
            return tar_file_path
        elif len(present_files) == 0:
            m = f'No files exist for {tar_file_path}, extracting all'
            m += f' {len(member_names)} files.'
            if verbose:print(m)
        else:
            m = f'Some files are missing for {tar_file_path}, extracting'
            m += f' {len(missing_files)} files. from total {len(member_names)}.'
            if verbose:print(m)
            member_names = missing_files
    stem = Path(tar_file_path).stem
    output_path = Path(output_path) / stem
    output_path.mkdir(parents=True, exist_ok=True)
    with tarfile.open(tar_file_path, 'r') as tar:
        extract_files_from_tar(tar, member_names, output_path, verbose)
    return tar_file_path

def list_contents(tar_file_path):
    print('listing files from tar:', tar_file_path)
    with tarfile.open(tar_file_path, 'r') as tar:
        fn = tar.getnames()
    return fn

def list_audio_files(tar_file_path, extension='.ogg'):
    fn = list_contents(tar_file_path)
    print(f'filtering audio files with extension {extension}')
    audio_files = [f for f in fn if f.endswith(extension)]
    return audio_files

def extract_file_from_tar(tar, member_name, output_path, verbose = False):
    if verbose:
        print(f'extracting {member_name} from {tar} to {output_path}')
    member = tar.getmember(member_name)
    tar.extract(member, path=output_path, filter = 'data')

def extract_files_from_tar(tar, member_names, output_path, verbose = False):
    for member_name in member_names:
        extract_file_from_tar(tar, member_name, output_path, verbose = False)


def check_whether_files_exist(tar_file_path, member_names, output_path):
    stem = Path(tar_file_path).stem
    output_path = Path(output_path) / stem
    present_files = []
    missing_files = []
    for member_name in member_names:
        output_file_path = output_path / member_name
        if output_file_path.exists() and output_file_path.stat().st_size == 0:
            output_file_path.unlink()
        if output_file_path.exists():
            present_files.append(member_name)
        else:
            missing_files.append(member_name)
    all_exist = len(missing_files) == 0 and len(present_files) == len(member_names)
    return all_exist, present_files, missing_files
    
    

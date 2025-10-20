from functools import partial
from multiprocessing import Pool
from progressbar import progressbar
import tarfile
import time
from tqdm import tqdm

def _runner(args, output_path, verbose):
    tar_file_path, member_names = args
    return extract_files(tar_file_path, member_names, output_path, verbose)

def extract_files_with_tar_audio_file_dict_mp(tar_audio_file_dict,
    output_path, num_workers = 64, verbose = False):
    items = tar_audio_file_dict.items()
    print(time.ctime(), len(items), num_workers)
    with Pool(processes=num_workers) as pool:
        results = []
        for r in tqdm(
            pool.imap_unordered(
                partial(_runner, output_path = output_path, verbose = verbose),
                items, chunksize = num_workers), 
                total= len(items)):
            results.append(r)
            print(r, 'done', len(results), len(items), time.ctime())

        

def extract_files_with_tar_audio_file_dict(tar_audio_file_dict, output_path, 
    verbose = False):
    for tar_file_path, member_names in progressbar(tar_audio_file_dict.items()):
        extract_files(tar_file_path, member_names, output_path, verbose)

def extract_files(tar_file_path, member_names, output_path, verbose = False):
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
        print(f'extracting {member_name} from {tar_file_path} to {output_path}')
    member = tar.getmember(member_name)
    tar.extract(member, path=output_path, filter = 'data')

def extract_files_from_tar(tar, member_names, output_path, verbose = False):
    for member_name in member_names:
        extract_file_from_tar(tar, member_name, output_path, verbose)

    
    

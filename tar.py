from progressbar import progressbar
import tarfile

def extract_files_with_tar_audio_file_dict(tar_audio_file_dict, output_path, 
    verbose = False):
    for tar_file_path, member_names in progressbar(tar_audio_file_dict.items()):
        extract_files(tar_file_path, member_names, output_path, verbose)

def extract_files(tar_file_path, member_names, output_path, verbose = False):
    with tarfile.open(tar_file_path, 'r') as tar:
        extract_files_from_tar(tar, member_names, output_path, verbose)

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
    tar.extract(member, path=output_path)

def extract_files_from_tar(tar, member_names, output_path, verbose = False):
    for member_name in member_names:
        extract_file_from_tar(tar, member_name, output_path, verbose)

    
    

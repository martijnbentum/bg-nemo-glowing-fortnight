from itertools import chain
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

test = BASE_DIR / "test"
validation = BASE_DIR / "validation"
train = BASE_DIR / "train"

test_filenames = list(test.glob("*.json"))
validations_filenames = list(validation.glob("*.json"))
train_filenames = list(train.glob("*.json"))


manifest_filenames = test_filenames + validations_filenames + train_filenames

csv_dir = BASE_DIR / "csv"
csv_test = csv_dir / "test_segments.csv"
csv_validation = csv_dir / "validation_segments.csv"
csv_train = csv_dir / "train_segments.csv"

csv_episode_dict_filename = BASE_DIR / "csv_episode_id_dict.json"

program_directory = BASE_DIR / "new_programs"
program_info_filename = BASE_DIR / "program_info.json"

manifest_program_directory = BASE_DIR / "manifest_programs"

hallucinations_directory = BASE_DIR / "hallucinations"

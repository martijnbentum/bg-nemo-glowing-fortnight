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

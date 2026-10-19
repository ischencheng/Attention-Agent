"""Export the frozen, unsolved repository without evaluator, solution, or Git history."""
from __future__ import annotations

import argparse
import io
from pathlib import Path
import shutil
import subprocess
import tarfile

BASELINE = "ed4d938ff89645fbeb1e58a10d66509425ede379"
HERE = Path(__file__).resolve().parent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("destination", type=Path)
    args = parser.parse_args()
    destination = args.destination.resolve()
    if destination.exists():
        parser.error("Destination already exists; choose a fresh directory to reset the task")
    archive = subprocess.run(["git", "-C", str(HERE.parents[1]), "archive", BASELINE],
                             check=True, capture_output=True).stdout
    destination.mkdir(parents=True)
    with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
        tar.extractall(destination, filter="data")
    shutil.copy2(HERE / "TASK.md", destination / "TASK.md")
    shutil.copy2(HERE / "requirements.txt", destination / "evaluation-requirements.txt")
    print(f"Prepared unsolved task at {destination}\nBase: {BASELINE}\nRead TASK.md to begin.")


if __name__ == "__main__":
    main()

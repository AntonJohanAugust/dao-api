import subprocess
import sys
import os


def start_project():
    subprocess.run(
        ["poetry", "install"],
        stdout=sys.stdout,
        stderr=sys.stderr,
        text=True,
        env=os.environ.copy(),
    )

    print("--- Starting uvicorn server ---")
    subprocess.run(
        ["poetry", "run", "uvicorn", "api_core.main:app"],
        stdout=sys.stdout,
        stderr=sys.stderr,
        text=True,
        env=os.environ.copy(),
    )


if __name__ == "__main__":
    start_project()

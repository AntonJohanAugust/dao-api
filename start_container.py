import subprocess
import sys
import os


def start_container():
    print("--- Loading docker image ---")
    with open("docker_image.tar.gz", "rb") as f:
        result = subprocess.run(
            ["docker", "load"],
            stdin=f,
            env=os.environ.copy(),
            check=True,
            capture_output=True,
            text=True,
        )
        print(result.stdout)

    print("--- Starting docker container ---")
    subprocess.run(
        [
            "docker",
            "run",
            "--name",
            "data_dao_de_task",
            "--rm",
            "--init",
            "data_dao_de_task",
            "http://host.docker.internal:8000",
        ],
        env=os.environ.copy(),
    )


if __name__ == "__main__":
    start_container()

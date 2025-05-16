import subprocess


def exec_bulk_load_scripts():
    try:
        print("Filling Neo4j using neo4j/load_dataset.py script")
        subprocess.run(["python3", "neo4j/load_dataset.py"], check=True)

        print("Filling BaseX using BaseX/Dataset.py script")
        subprocess.run(["python3", "basex/load_dataset.py"], check=True)

    except subprocess.CalledProcessError as e:
        print(f"Error occurred while executing a script: {e}")


if __name__ == "__main__":
    exec_bulk_load_scripts()

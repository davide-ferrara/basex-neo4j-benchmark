import subprocess

def run_benchmark():
    try:
        print("Running basex/queries.py script")
        subprocess.run(["python", "basex/queries.py"], check=True)

        print("Running neo4j/queries.py script")
        subprocess.run(["python", "neo4j/queries.py"], check=True)

        print("Running generating_histograms.py script")
        subprocess.run(["python", "generating_histograms.py"], check=True)

    except subprocess.CalledProcessError as e:
        print(f"Error occurred while executing a script: {e}")


if __name__ == "__main__":
    run_benchmark()

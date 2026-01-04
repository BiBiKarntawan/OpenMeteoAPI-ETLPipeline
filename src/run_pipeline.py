# src/run_pipeline.py
import subprocess
import sys

STEPS = [
    ("Extract", [sys.executable, "src/extract.py"]),
    ("Transform", [sys.executable, "src/transform.py"]),
    ("Validate (GX)", [sys.executable, "src/validate_gx.py"]),
    ("Load", [sys.executable, "src/load.py"]),
    ("Analytics", [sys.executable, "src/analytics.py"]),
]

def main():
    for name, cmd in STEPS:
        print(f"\n=== {name} ===")
        result = subprocess.run(cmd)
        if result.returncode != 0:
            print(f"\n❌ Pipeline stopped at step: {name}")
            sys.exit(result.returncode)

    print("\n Pipeline completed successfully!")

if __name__ == "__main__":
    main()

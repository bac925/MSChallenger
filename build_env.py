import os
import sys
import subprocess

MIN_PYTHON = (3, 10)

def run(cmd):
    print(f"[RUN] {cmd}")
    subprocess.check_call(cmd, shell=True)

def main():
    if sys.version_info < MIN_PYTHON:
        print(f"[ERROR] Python {MIN_PYTHON[0]}.{MIN_PYTHON[1]}+ required.")
        sys.exit(1)

    if not os.path.exists("venv"):
        print("[INFO] Creating virtual environment...")
        run(f"{sys.executable} -m venv venv")
    else:
        print("[INFO] venv already exists.")

    pip = r"venv\Scripts\pip.exe" if os.name == "nt" else "venv/bin/pip"

    print("[INFO] Upgrading pip...")
    run(f"{pip} install --upgrade pip")

    print("[INFO] Installing requirements...")
    run(f"{pip} install -r requirements.txt")

    print("[SUCCESS] Environment setup complete.")
    print("You can now run start.bat")

if __name__ == "__main__":
    main()

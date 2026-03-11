from pathlib import Path


def main():
    project_root = Path(__file__).resolve().parents[2]
    raw_data_dir = project_root / "data" / "raw"

    print(f"Project root: {project_root}")
    print(f"Raw data directory: {raw_data_dir}")

    if not raw_data_dir.exists():
        print("Raw data directory does not exist yet.")
        return

    files = list(raw_data_dir.rglob("*"))
    if not files:
        print("Raw data directory is empty.")
        return

    print("\nFiles found in data/raw:")
    for path in files:
        if path.is_file():
            print(path.relative_to(project_root))


if __name__ == "__main__":
    main()
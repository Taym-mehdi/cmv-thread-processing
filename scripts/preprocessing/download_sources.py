from pathlib import Path
import requests

SOURCE_URLS = {
    "cmv_dataset_page.html": "https://chenhaot.com/papers/changemyview/",
    "winning_arguments_paper.pdf": "https://www.cs.cornell.edu/~cristian/pdfs/winning_arguments.pdf",
    "egawa_2019.pdf": "https://aclanthology.org/P19-2059.pdf",
    "falenska_2024.pdf": "https://aclanthology.org/2024.lrec-main.1272.pdf",
}


def download_file(url: str, output_path: Path) -> None:
    print(f"Downloading: {url}")
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    output_path.write_bytes(response.content)
    print(f"Saved to: {output_path}")


def main():
    project_root = Path(__file__).resolve().parents[2]
    raw_dir = project_root / "data" / "raw"

    cmv_dir = raw_dir / "cmv"
    annotations_dir = raw_dir / "annotations"

    cmv_dir.mkdir(parents=True, exist_ok=True)
    annotations_dir.mkdir(parents=True, exist_ok=True)

    file_targets = {
        "cmv_dataset_page.html": cmv_dir / "cmv_dataset_page.html",
        "winning_arguments_paper.pdf": cmv_dir / "winning_arguments_paper.pdf",
        "egawa_2019.pdf": annotations_dir / "egawa_2019.pdf",
        "falenska_2024.pdf": annotations_dir / "falenska_2024.pdf",
    }

    for filename, url in SOURCE_URLS.items():
        output_path = file_targets[filename]
        if output_path.exists():
            print(f"Skipping existing file: {output_path}")
            continue
        download_file(url, output_path)


if __name__ == "__main__":
    main()
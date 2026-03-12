from pathlib import Path
import pandas as pd


def main():
    project_root = Path(__file__).resolve().parents[2]
    input_path = project_root / "data" / "interim" / "cmv" / "comments.csv"
    output_path = project_root / "data" / "interim" / "cmv" / "comments_clean.csv"

    print("Loading comments...")
    df = pd.read_csv(input_path)

    print(f"Original rows: {len(df):,}")
    print(f"Original unique comment IDs: {df['comment_id'].nunique():,}")

    placeholder_mask = df["comment_id"] == "_"
    num_placeholders = placeholder_mask.sum()

    print(f"Rows with placeholder comment_id '_': {num_placeholders:,}")

    df["has_synthetic_comment_id"] = False

    placeholder_indices = df.index[placeholder_mask].tolist()

    for i, idx in enumerate(placeholder_indices, start=1):
        new_id = f"deleted_{i:06d}"
        df.at[idx, "comment_id"] = new_id
        df.at[idx, "comment_name"] = f"t1_{new_id}"
        df.at[idx, "has_synthetic_comment_id"] = True

    print(f"New unique comment IDs: {df['comment_id'].nunique():,}")
    print(f"Duplicate comment IDs after fix: {len(df) - df['comment_id'].nunique():,}")

    df.to_csv(output_path, index=False)

    print(f"Saved cleaned comments to: {output_path}")


if __name__ == "__main__":
    main()
from pathlib import Path
import pandas as pd


def compute_depths(comments_df):
    parent_map = dict(zip(comments_df["comment_name"], comments_df["parent_id"]))
    depth_map = {}

    def get_depth(comment_name):
        if comment_name in depth_map:
            return depth_map[comment_name]

        parent = parent_map.get(comment_name)

        if pd.isna(parent):
            depth = 0
        elif parent.startswith("t3_"):  # parent is thread post
            depth = 1
        else:
            depth = 1 + get_depth(parent)

        depth_map[comment_name] = depth
        return depth

    depths = []
    for name in comments_df["comment_name"]:
        depths.append(get_depth(name))

    comments_df["depth"] = depths
    return comments_df


def main():
    project_root = Path(__file__).resolve().parents[2]
    comments_path = project_root / "data" / "interim" / "cmv" / "comments_clean.csv"
    output_path = project_root / "data" / "processed" / "cmv" / "comments_with_depth.csv"

    print("Loading comments...")
    df = pd.read_csv(comments_path)

    print("Computing depth...")
    df = compute_depths(df)

    df.to_csv(output_path, index=False)

    print(f"Saved to: {output_path}")

    print("\nDepth statistics:")
    print("Mean depth:", df["depth"].mean())
    print("Median depth:", df["depth"].median())
    print("Max depth:", df["depth"].max())


if __name__ == "__main__":
    main()
from pathlib import Path
import pandas as pd


def main():
    project_root = Path(__file__).resolve().parents[2]
    comments_path = project_root / "data" / "interim" / "cmv" / "comments_clean.csv"
    threads_path = project_root / "data" / "interim" / "cmv" / "threads.csv"

    print("Loading cleaned comments...")
    comments_df = pd.read_csv(comments_path)
    threads_df = pd.read_csv(threads_path)

    print(f"Rows: {len(comments_df):,}")
    print(f"Unique comment IDs: {comments_df['comment_id'].nunique():,}")
    print(f"Duplicate comment IDs: {len(comments_df) - comments_df['comment_id'].nunique():,}")
    print(f"Synthetic comment IDs: {comments_df['has_synthetic_comment_id'].sum():,}")

    thread_names = set(comments_df["thread_name"].dropna())
    comment_names = set(comments_df["comment_name"].dropna())
    parent_ids = comments_df["parent_id"].dropna()

    parent_is_thread = parent_ids.isin(thread_names).sum()
    parent_is_comment = parent_ids.isin(comment_names).sum()
    parent_unresolved = len(parent_ids) - parent_is_thread - parent_is_comment

    print(f"Parent IDs pointing to threads: {parent_is_thread:,}")
    print(f"Parent IDs pointing to comments: {parent_is_comment:,}")
    print(f"Unresolved parent IDs: {parent_unresolved:,}")

    missing_thread_refs = set(comments_df["thread_id"]) - set(threads_df["thread_id"])
    print(f"Missing thread references: {len(missing_thread_refs):,}")


if __name__ == "__main__":
    main()
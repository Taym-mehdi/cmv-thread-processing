from pathlib import Path
import pandas as pd


def main():
    project_root = Path(__file__).resolve().parents[2]
    threads_path = project_root / "data" / "interim" / "cmv" / "threads.csv"
    comments_path = project_root / "data" / "interim" / "cmv" / "comments_clean.csv"
    output_path = project_root / "data" / "processed" / "cmv" / "thread_stats.csv"

    print("Loading data...")
    threads_df = pd.read_csv(threads_path)
    comments_df = pd.read_csv(comments_path)

    print("Computing per-thread statistics...")

    grouped = comments_df.groupby("thread_id")

    stats_df = grouped.agg(
        total_comments=("comment_id", "count"),
        top_level_comments=("is_top_level", "sum"),
        op_comments=("author_is_op", "sum"),
        deleted_removed_comments=("is_deleted_or_removed", "sum"),
        first_comment_time=("created_utc", "min"),
        last_comment_time=("created_utc", "max"),
    ).reset_index()

    stats_df["thread_duration_seconds"] = (
        stats_df["last_comment_time"] - stats_df["first_comment_time"]
    )

    stats_df["avg_comments_per_top_level_branch"] = (
        stats_df["total_comments"] / stats_df["top_level_comments"]
    )

    stats_df = stats_df.merge(
        threads_df[["thread_id", "title", "op_author", "created_utc", "num_comments_raw"]],
        on="thread_id",
        how="left",
    )

    stats_df = stats_df.rename(columns={"created_utc": "thread_created_utc"})

    output_path.parent.mkdir(parents=True, exist_ok=True)
    stats_df.to_csv(output_path, index=False)

    print(f"Saved thread stats to: {output_path}")
    print(f"Rows: {len(stats_df):,}")

    print("\nQuick summary:")
    print(f"Mean comments per thread: {stats_df['total_comments'].mean():.2f}")
    print(f"Median comments per thread: {stats_df['total_comments'].median():.2f}")
    print(f"Max comments in a thread: {stats_df['total_comments'].max():,}")
    print(f"Mean top-level branches: {stats_df['top_level_comments'].mean():.2f}")
    print(f"Mean deleted/removed comments: {stats_df['deleted_removed_comments'].mean():.2f}")


if __name__ == "__main__":
    main()
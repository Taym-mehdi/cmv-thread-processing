from pathlib import Path
import pandas as pd
import re


DELTA_PATTERNS = [
    r"∆",               # delta symbol variant
    r"Δ",               # greek delta
    r"\bdelta\b",       # word delta
    r"&#8710;",         # html entity if it appears
]


def contains_delta_marker(text: str) -> bool:
    if not isinstance(text, str):
        return False

    for pattern in DELTA_PATTERNS:
        if re.search(pattern, text, flags=re.IGNORECASE):
            return True

    return False


def main():
    project_root = Path(__file__).resolve().parents[2]

    threads_path = project_root / "data" / "interim" / "cmv" / "threads.csv"
    comments_path = project_root / "data" / "interim" / "cmv" / "comments_clean.csv"

    output_dir = project_root / "data" / "processed" / "cmv"
    output_dir.mkdir(parents=True, exist_ok=True)

    delta_comments_output = output_dir / "delta_comments.csv"
    thread_deltas_output = output_dir / "thread_deltas.csv"

    print("Loading data...")
    threads_df = pd.read_csv(threads_path)
    comments_df = pd.read_csv(comments_path)

    print("Detecting delta markers in comments...")
    comments_df["contains_delta_marker"] = comments_df["body"].apply(contains_delta_marker)

    delta_comments_df = comments_df[comments_df["contains_delta_marker"]].copy()

    print("Filtering likely real delta awards...")
    # strongest signal: OP wrote the comment and it contains a delta marker
    delta_comments_df["is_op_delta_comment"] = delta_comments_df["author_is_op"] & delta_comments_df["contains_delta_marker"]

    thread_delta_stats = (
        delta_comments_df.groupby("thread_id")
        .agg(
            delta_comment_count=("comment_id", "count"),
            op_delta_comment_count=("is_op_delta_comment", "sum"),
            first_delta_comment_time=("created_utc", "min"),
        )
        .reset_index()
    )

    thread_delta_stats["thread_has_any_delta_marker"] = thread_delta_stats["delta_comment_count"] > 0
    thread_delta_stats["thread_has_op_delta"] = thread_delta_stats["op_delta_comment_count"] > 0

    # merge with all threads so threads with no delta also appear
    thread_delta_stats = threads_df[["thread_id", "title", "op_author", "created_utc"]].merge(
        thread_delta_stats,
        on="thread_id",
        how="left",
    )

    thread_delta_stats["delta_comment_count"] = thread_delta_stats["delta_comment_count"].fillna(0).astype(int)
    thread_delta_stats["op_delta_comment_count"] = thread_delta_stats["op_delta_comment_count"].fillna(0).astype(int)
    thread_delta_stats["thread_has_any_delta_marker"] = thread_delta_stats["thread_has_any_delta_marker"].fillna(False)
    thread_delta_stats["thread_has_op_delta"] = thread_delta_stats["thread_has_op_delta"].fillna(False)

    delta_comments_df.to_csv(delta_comments_output, index=False)
    thread_delta_stats.to_csv(thread_deltas_output, index=False)

    print(f"Saved delta comments to: {delta_comments_output}")
    print(f"Saved thread delta stats to: {thread_deltas_output}")

    print("\nQuick summary:")
    print(f"Threads total: {len(thread_delta_stats):,}")
    print(f"Threads with any delta marker: {thread_delta_stats['thread_has_any_delta_marker'].sum():,}")
    print(f"Threads with OP delta: {thread_delta_stats['thread_has_op_delta'].sum():,}")
    print(f"Total comments containing delta marker: {len(delta_comments_df):,}")
    print(f"Comments with OP delta marker: {delta_comments_df['is_op_delta_comment'].sum():,}")


if __name__ == "__main__":
    main()
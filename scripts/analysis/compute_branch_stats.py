from pathlib import Path
import pandas as pd


def assign_branch_roots(comments_df: pd.DataFrame) -> pd.DataFrame:
    parent_map = dict(zip(comments_df["comment_name"], comments_df["parent_id"]))
    id_map = dict(zip(comments_df["comment_name"], comments_df["comment_id"]))
    author_map = dict(zip(comments_df["comment_name"], comments_df["author"]))

    root_cache = {}

    def get_root_top_level_comment(comment_name: str) -> str:
        if comment_name in root_cache:
            return root_cache[comment_name]

        parent_id = parent_map.get(comment_name)

        if pd.isna(parent_id):
            root = comment_name
        elif isinstance(parent_id, str) and parent_id.startswith("t3_"):
            # direct reply to thread post => this comment is itself the branch root
            root = comment_name
        else:
            root = get_root_top_level_comment(parent_id)

        root_cache[comment_name] = root
        return root

    comments_df["branch_root_comment_name"] = comments_df["comment_name"].apply(get_root_top_level_comment)
    comments_df["branch_root_comment_id"] = comments_df["branch_root_comment_name"].map(id_map)
    comments_df["branch_root_author"] = comments_df["branch_root_comment_name"].map(author_map)

    return comments_df


def main():
    project_root = Path(__file__).resolve().parents[2]

    comments_path = project_root / "data" / "processed" / "cmv" / "comments_with_depth.csv"
    thread_deltas_path = project_root / "data" / "processed" / "cmv" / "thread_deltas.csv"

    output_dir = project_root / "data" / "processed" / "cmv"
    output_dir.mkdir(parents=True, exist_ok=True)

    comments_branches_output = output_dir / "comments_with_branches.csv"
    branch_stats_output = output_dir / "branch_stats.csv"

    print("Loading comments with depth...")
    comments_df = pd.read_csv(comments_path)

    print("Loading thread delta stats...")
    thread_deltas_df = pd.read_csv(thread_deltas_path)

    print("Assigning branch roots...")
    comments_df = assign_branch_roots(comments_df)

    print("Computing branch-level statistics...")
    branch_stats_df = (
        comments_df.groupby(["thread_id", "branch_root_comment_name"], as_index=False)
        .agg(
            branch_root_comment_id=("branch_root_comment_id", "first"),
            branch_root_author=("branch_root_author", "first"),
            branch_size=("comment_id", "count"),
            branch_max_depth=("depth", "max"),
            branch_mean_depth=("depth", "mean"),
            op_comments_in_branch=("author_is_op", "sum"),
            deleted_removed_comments_in_branch=("is_deleted_or_removed", "sum"),
            first_comment_time=("created_utc", "min"),
            last_comment_time=("created_utc", "max"),
        )
    )

    branch_stats_df["op_participates_in_branch"] = branch_stats_df["op_comments_in_branch"] > 0
    branch_stats_df["branch_duration_seconds"] = (
        branch_stats_df["last_comment_time"] - branch_stats_df["first_comment_time"]
    )

    print("Keeping delta information...")
    # mark whether this thread has an OP delta at all
    branch_stats_df = branch_stats_df.merge(
        thread_deltas_df[["thread_id", "thread_has_op_delta", "op_delta_comment_count"]],
        on="thread_id",
        how="left",
    )

    # branch-level delta approximation:
    # if OP participates in a delta thread, this branch may be especially relevant
    branch_stats_df["branch_in_delta_thread"] = branch_stats_df["thread_has_op_delta"].fillna(False)

    print("Saving outputs...")
    comments_df.to_csv(comments_branches_output, index=False)
    branch_stats_df.to_csv(branch_stats_output, index=False)

    print(f"Saved comments with branches to: {comments_branches_output}")
    print(f"Saved branch stats to: {branch_stats_output}")

    print("\nQuick summary:")
    print(f"Total branches: {len(branch_stats_df):,}")
    print(f"Mean branch size: {branch_stats_df['branch_size'].mean():.2f}")
    print(f"Median branch size: {branch_stats_df['branch_size'].median():.2f}")
    print(f"Max branch size: {branch_stats_df['branch_size'].max():,}")
    print(f"Mean branch max depth: {branch_stats_df['branch_max_depth'].mean():.2f}")
    print(f"Branches with OP participation: {branch_stats_df['op_participates_in_branch'].sum():,}")
    print(f"Branches inside delta threads: {branch_stats_df['branch_in_delta_thread'].sum():,}")


if __name__ == "__main__":
    main()
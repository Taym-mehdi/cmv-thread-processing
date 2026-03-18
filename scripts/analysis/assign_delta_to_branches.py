from pathlib import Path
import pandas as pd


def main():
    project_root = Path(__file__).resolve().parents[2]

    comments_path = project_root / "data" / "processed" / "cmv" / "comments_with_branches.csv"
    delta_comments_path = project_root / "data" / "processed" / "cmv" / "delta_comments.csv"
    branch_stats_path = project_root / "data" / "processed" / "cmv" / "branch_stats.csv"

    output_path = project_root / "data" / "processed" / "cmv" / "branch_stats_with_delta.csv"

    print("Loading data...")
    comments_df = pd.read_csv(comments_path)
    delta_comments_df = pd.read_csv(delta_comments_path)
    branch_stats_df = pd.read_csv(branch_stats_path)

    print("Filtering OP delta comments...")
    op_delta_comments = delta_comments_df[delta_comments_df["is_op_delta_comment"] == True]

    print("Mapping comment_name → branch root...")
    comment_to_branch = dict(
        zip(comments_df["comment_name"], comments_df["branch_root_comment_name"])
    )

    print("Assigning delta to branches...")
    branch_delta_counts = {}

    for _, row in op_delta_comments.iterrows():
        parent_id = row["parent_id"]

        if isinstance(parent_id, str) and parent_id.startswith("t1_"):
            branch_root = comment_to_branch.get(parent_id)

            if branch_root:
                key = (row["thread_id"], branch_root)
                branch_delta_counts[key] = branch_delta_counts.get(key, 0) + 1

    print("Adding delta info to branch stats...")

    def get_branch_delta_count(row):
        key = (row["thread_id"], row["branch_root_comment_name"])
        return branch_delta_counts.get(key, 0)

    branch_stats_df["branch_op_delta_count"] = branch_stats_df.apply(get_branch_delta_count, axis=1)
    branch_stats_df["branch_has_op_delta"] = branch_stats_df["branch_op_delta_count"] > 0

    branch_stats_df.to_csv(output_path, index=False)

    print(f"Saved to: {output_path}")

    print("\nSummary:")
    print(f"Total branches: {len(branch_stats_df):,}")
    print(f"Branches with OP delta: {branch_stats_df['branch_has_op_delta'].sum():,}")
    print(f"Total OP delta assignments: {branch_stats_df['branch_op_delta_count'].sum():,}")


if __name__ == "__main__":
    main()
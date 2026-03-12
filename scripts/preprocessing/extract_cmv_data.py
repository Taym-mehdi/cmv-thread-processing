from pathlib import Path
import gzip
import json
import pandas as pd
from tqdm import tqdm


def normalize_text(value):
    if value is None:
        return "[missing]"
    if isinstance(value, str):
        stripped = value.strip()
        if stripped == "":
            return "[empty]"
        return stripped
    return str(value)


def is_deleted_or_removed(body, author):
    body_str = (body or "").strip() if isinstance(body, str) else ""
    deleted_markers = {"[deleted]", "[removed]"}
    return body_str in deleted_markers or author is None


def extract_thread_row(thread, source_file):
    return {
        "thread_id": thread.get("id"),
        "thread_name": thread.get("name"),
        "title": normalize_text(thread.get("title")),
        "op_author": thread.get("author"),
        "op_body": normalize_text(thread.get("selftext")),
        "created_utc": thread.get("created_utc"),
        "num_comments_raw": thread.get("num_comments"),
        "score": thread.get("score"),
        "subreddit": thread.get("subreddit"),
        "permalink": thread.get("permalink"),
        "source_file": source_file,
    }


def extract_comment_rows(thread, source_file):
    rows = []
    thread_id = thread.get("id")
    thread_name = thread.get("name")
    op_author = thread.get("author")
    comments = thread.get("comments", [])

    for comment in comments:
        body = comment.get("body")
        author = comment.get("author")
        deleted_or_removed = is_deleted_or_removed(body, author)

        if deleted_or_removed:
            body_clean = "[deleted_or_removed]"
        else:
            body_clean = normalize_text(body)

        parent_id = comment.get("parent_id")
        is_top_level = parent_id == thread_name
        author_is_op = author == op_author if author is not None else False

        rows.append(
            {
                "thread_id": thread_id,
                "thread_name": thread_name,
                "comment_id": comment.get("id"),
                "comment_name": comment.get("name"),
                "parent_id": parent_id,
                "author": author,
                "body": body_clean,
                "created_utc": comment.get("created_utc"),
                "score": comment.get("score"),
                "subreddit": comment.get("subreddit"),
                "link_id": comment.get("link_id"),
                "is_top_level": is_top_level,
                "author_is_op": author_is_op,
                "is_deleted_or_removed": deleted_or_removed,
                "source_file": source_file,
            }
        )

    return rows


def main():
    project_root = Path(__file__).resolve().parents[2]
    input_path = project_root / "data" / "raw" / "cmv" / "cmv_20161111.jsonlist.gz"
    output_dir = project_root / "data" / "interim" / "cmv"
    output_dir.mkdir(parents=True, exist_ok=True)

    threads_output = output_dir / "threads.csv"
    comments_output = output_dir / "comments.csv"

    thread_rows = []
    comment_rows = []

    with gzip.open(input_path, "rt", encoding="utf-8") as f:
        for line in tqdm(f, desc="Processing threads"):
            line = line.strip()
            if not line:
                continue

            thread = json.loads(line)
            thread_rows.append(extract_thread_row(thread, input_path.name))
            comment_rows.extend(extract_comment_rows(thread, input_path.name))

    threads_df = pd.DataFrame(thread_rows)
    comments_df = pd.DataFrame(comment_rows)

    threads_df.to_csv(threads_output, index=False)
    comments_df.to_csv(comments_output, index=False)

    print("\nDone.")
    print(f"Threads saved to: {threads_output}")
    print(f"Comments saved to: {comments_output}")
    print(f"Number of threads: {len(threads_df)}")
    print(f"Number of comments: {len(comments_df)}")


if __name__ == "__main__":
    main()
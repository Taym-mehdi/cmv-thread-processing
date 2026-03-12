# CMV Thread Processing

This project reconstructs the thread structure of the r/ChangeMyView dataset and computes structural statistics for argumentative discussions.

## Dataset
CMV Reddit dump: `cmv_20161111.jsonlist.gz`

## Pipeline

### 1. Inspect raw data
`scripts/preprocessing/inspect_raw_data.py`

Checks the contents of the raw data directory.

### 2. Extract raw CMV data
`scripts/preprocessing/extract_cmv_data.py`

Reads the compressed CMV dump and produces:
- `threads.csv`
- `comments.csv`

### 3. Clean comment IDs
`scripts/preprocessing/clean_comments.py`

Replaces placeholder deleted-comment IDs (`_`) with unique synthetic IDs.

Produces:
- `comments_clean.csv`

### 4. Validate cleaned comments
`scripts/preprocessing/validate_clean_comments.py`

Checks:
- unique comment IDs
- valid parent references
- no missing thread references

### 5. Compute thread statistics
`scripts/analysis/compute_thread_stats.py`

Produces per-thread metrics such as:
- total comments
- top-level branches
- OP replies
- deleted/removed comments
- thread duration

Output:
- `thread_stats.csv`

### 6. Compute comment depth
`scripts/analysis/compute_comment_depth.py`

Reconstructs the reply tree using parent links and computes comment depth.

Output:
- `comments_with_depth.csv`

## Key statistics

- Threads extracted: **25,043**
- Comments extracted: **1,526,056**
- Mean comments per thread: **61.25**
- Median comments per thread: **37**
- Max comments in a thread: **1,781**
- Mean top-level branches per thread: **12.05**
- Mean comment depth: **3.92**
- Median comment depth: **3**
- Max comment depth: **11**

## Notes
Raw, interim, and processed data files are kept locally and ignored by Git.
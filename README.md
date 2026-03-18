# CMV Thread Processing

This project reconstructs the thread structure of the r/ChangeMyView dataset and computes structural statistics for argumentative discussions.

## Dataset
CMV Reddit dump: `cmv_20161111.jsonlist.gz`

## Pipeline

### 1. Inspect raw data
`scripts/preprocessing/inspect_raw_data.py`

Checks the contents of the raw data directory.

---

### 2. Extract raw CMV data
`scripts/preprocessing/extract_cmv_data.py`

Reads the compressed CMV dump and produces:
- `threads.csv`
- `comments.csv`

---

### 3. Clean comment IDs
`scripts/preprocessing/clean_comments.py`

Replaces placeholder deleted-comment IDs (`_`) with unique synthetic IDs.

Produces:
- `comments_clean.csv`

---

### 4. Validate cleaned comments
`scripts/preprocessing/validate_clean_comments.py`

Ensures:
- unique comment IDs
- valid parent references
- no missing thread links

---

### 5. Compute thread statistics
`scripts/analysis/compute_thread_stats.py`

Produces per-thread metrics:
- total comments
- top-level branches
- OP replies
- deleted/removed comments
- thread duration

Output:
- `thread_stats.csv`

---

### 6. Compute comment depth
`scripts/analysis/compute_comment_depth.py`

Reconstructs the reply tree and computes discussion depth.

Output:
- `comments_with_depth.csv`

---

### 7. Detect persuasion (delta detection)
`scripts/analysis/detect_deltas.py`

Detects delta markers in comments.

Outputs:
- `delta_comments.csv`
- `thread_deltas.csv`

---

### 8. Branch-level analysis
`scripts/analysis/compute_branch_stats.py`

Splits threads into sub-discussions (branches).

Each top-level comment defines a branch.

Outputs:
- `comments_with_branches.csv`
- `branch_stats.csv`

---

### 9. Assign delta to branches (NEW)
`scripts/analysis/assign_delta_to_branches.py`

Identifies which specific branch convinced the OP.

Outputs:
- `branch_stats_with_delta.csv`

---

## Key statistics

- Threads: **25,043**
- Comments: **1,526,056**

### Thread-level
- Mean comments per thread: **61.25**
- Median comments per thread: **37**
- Max comments: **1,781**
- Mean branches per thread: **12.05**

### Depth
- Mean depth: **3.92**
- Median depth: **3**
- Max depth: **11**

### Branch-level
- Total branches: **300,151**
- Mean branch size: **5.08**
- Median branch size: **2**
- Max branch size: **958**
- Mean branch depth: **2.97**

---

## Research value

The dataset now supports analysis of:

- discussion structure (depth, branching)
- OP participation
- deleted/removed content
- persuasion success via delta awards
- branch-level argument effectiveness

This enables studying how constructiveness and persuasion relate to discussion structure.

---

## Notes

Raw, interim, and processed data are stored locally and ignored by Git.
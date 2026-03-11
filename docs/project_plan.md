# Project Plan

## Main objective
Process the CMV raw Reddit dump into a thread-structured dataset for constructiveness research.

## Core tasks
- Extract all raw data
- Preserve thread IDs, comment IDs, parent IDs, and timestamps
- Reconstruct chronological and tree structure
- Keep deleted/removed comments with dummy values
- Derive subthreads from top-level replies
- Compute statistics about thread length, branching, and repeated topics
- Incorporate external annotations where possible
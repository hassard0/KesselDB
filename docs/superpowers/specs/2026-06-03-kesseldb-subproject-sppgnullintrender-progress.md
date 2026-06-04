# SP-PG-NULL-INT-RENDER — progress tracker

**Status:** CLOSED
**Date:** 2026-06-03

## Tasks
- [x] Diagnose the exact failure layer (render layer — non-sorted projection)
- [x] Confirm INSERT lowering sets `Value::Null` for omitted nullable (CORRECT)
- [x] Confirm codec `encode`/`decode` honor the null bitmap (CORRECT)
- [x] Confirm `SELECT *` (`emit_data_rows`/`decode_record`) renders NULL (CORRECT)
- [x] Fix: re-project non-sorted projection via `SELECT *` full records (root, render-only)
- [x] Add `kessel_sql::select_projection_to_star` + token-boundary FROM finder
- [x] Make `emit_projected_from_full_records` robust to bare-record (GetById) shape
- [x] Add explicit `NULL` literal support to INSERT VALUES (`Lit::Null`)
- [x] Generic across kinds (int + TEXT/CHAR), NOT-NULL/PK back-compat preserved
- [x] Unit tests (kessel-sql rewrite helper + boundary cases)
- [x] New psql smoke `scripts/sppgnullintrender-smoke.py`
- [x] Workspace test green on vulcan (exit 0)
- [x] Regression: relationships + realapp + fk-enforce smokes green
- [x] Docs: USAGE / STATUS / CHANGELOG
- [x] Closure commit on origin/main + final git-log proof
- [x] vulcan cleanup (worktree + target dir + data dirs)

## Transcripts

### Workspace test (vulcan, `CARGO_TARGET_DIR=/tmp/kdb-t-nz cargo test --workspace --release`)
```
<<<WORKSPACE_TEST_TAIL>>>
```

### Regression smoke — sppgormrelationships
```
<<<REL_SMOKE>>>
```

### Regression smoke — sppgormrealapp
```
<<<REALAPP_SMOKE>>>
```

### Regression smoke — sppgddlfkenforce
```
<<<FKENFORCE_SMOKE>>>
```

### NEW smoke — sppgnullintrender (real psycopg2 transcript)
```
<<<NULLINTRENDER_SMOKE>>>
```

## CLOSED
All asserts green; fix is on origin/main; vulcan cleaned up.

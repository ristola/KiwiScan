# KiwiScan v0.1.19

Release date: 2026-04-12

## Highlights

- Manual Mode now clears receivers once and parks the auto-set loop instead of continuing to churn in the background.
- Receiver assignment apply now supports cancellation, so switching to Manual Mode can supersede stale in-flight startup work.
- Manual Mode shutdown now prefers graceful worker teardown before admin kick fallback.
- System status now exposes unexpected managed Kiwi users separately so the UI can show when Kiwi still has external AUTO/FIXED/ROAM sessions.
- Deprecated WSPR band-hop controls were removed from the active backend and Pro UI paths.
- The legacy `index.html` UI was removed; the Pro UI remains the supported frontend.

## Validation

- Manual Mode was revalidated live after removing the stale laptop KiwiScan container that was recreating external AUTO receivers.
- KiwiScan restarted cleanly in Manual Mode with zero active receivers and no `AUTO_*` users reappearing during the verification window.

## Notes

- This release includes both `src/` and `prod_minimal/` runtime/frontend updates so the two app variants stay aligned.
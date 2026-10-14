# sec-edgar

## Parent: investment-assist

This repo is one of the investment data fetchers under `../investment-assist`, the parent and
only entry point of the family. It feeds layers 0, 2, 3 and 4 (FRED regime and US macro, TIC flows, US fundamentals) of the decision flow in
`../investment-assist/CLAUDE.md`.

- Fetch and store only. Never call or import a sibling repo; only investment-assist reads across
  the repos, read-only.
- The parent never fetches. Data the flow needs from this repo's sources is added here.
- When a table or file the parent reads is added, renamed or dropped, note it in this repo's plan
  so the matching adapter in investment-assist can follow.
- Cross-repo priorities, coverage gaps and build order live in the parent; check them before
  starting new work here.

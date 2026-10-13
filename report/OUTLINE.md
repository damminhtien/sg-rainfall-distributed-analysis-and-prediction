# Report status — Singapore Rainfall Analysis and Prediction

Updated 13 October 2026. The final report source is `main.tex`; it describes
the measured run `20261012-full-02`, not the earlier `full-01` draft. The cover
and PDF metadata use the exact project title from the assignment.

## Completed sections

- Abstract and assignment scope.
- Data source, cleaning counts, coverage rules and the early-2017 `:59`
  timestamp convention, its one-second analytical alignment, and the
  publisher's undocumented reason for the later `:00` convention.
- Task 1 annual, monthly and hourly findings for 2017–2024, including the
  empty common annual station cohort and notebook-generated figures.
- Task 2 target, features, temporal partitions, model comparison, calibration,
  2024 holdout, unseen thresholds, monotonicity, A/B feature-set comparison
  and final GBT feature-family importance; exploratory weekly-block uncertainty
  is in the appendix.
- Reproducibility hashes, AI-use declaration and limitations.

## Final checks

- [x] Compile the 16-page report and review the table, reference and figure
  layouts at submission size.
- [x] Fresh Spark-session load and one-row inference succeeded; evidence is in
  `tables/full-20261012/model_reload_validation.json`.
- [ ] Check the final package against the assignment upload requirements and
  retain the Canvas receipt after submission.

Notebook 00's local execution outputs are still missing. The downloaded model
artifacts did pass the fresh-session load and inference smoke check. See
`../data/TASK.md` for the full evidence checklist.

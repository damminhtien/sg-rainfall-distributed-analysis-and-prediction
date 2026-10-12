# Report status — DSA5208 Project 2

Updated 12 October 2026. The final report source is `main.tex`; it describes
the measured run `20261012-full-02`, not the earlier `full-01` draft.

## Completed sections

- Abstract and assignment scope.
- Data source, cleaning counts, coverage rules and the 2017 one-second epoch
  adjustment, stated as an unverified analytical assumption.
- Task 1 annual, monthly and hourly findings for 2017–2024, including the
  empty common annual station cohort and notebook-generated figures.
- Task 2 target, features, temporal partitions, model comparison, calibration,
  2024 holdout, unseen thresholds, monotonicity and weekly-block uncertainty.
- Reproducibility hashes, AI-use declaration and limitations.

## Final checks

- [x] Compile the 12-page report and review the table, reference and figure
  layouts at submission size.
- [x] Fresh Spark-session load and one-row inference succeeded; evidence is in
  `tables/full-20261012/model_reload_validation.json`.
- [ ] Check the final package against the assignment upload requirements and
  retain the Canvas receipt after submission.

Notebook 00's local execution outputs are still missing. The downloaded model
artifacts did pass the fresh-session load and inference smoke check. See
`../data/TASK.md` for the full evidence checklist.

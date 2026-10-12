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

- [ ] Compile the report and inspect every page and figure at submission size.
- [ ] Keep the fresh Spark-session model reload marked as unverified until a
  real load and inference check succeeds.
- [ ] Check the final package against the assignment upload requirements and
  retain the Canvas receipt after submission.

The report does not claim that Notebook 00 saved execution outputs or that the
downloaded Spark models have been reloaded in a fresh session. See
`../data/TASK.md` for the full evidence checklist.

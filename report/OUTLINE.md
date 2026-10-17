# Report outline: Singapore Rainfall Analysis and Prediction

The report presents eight years of Singapore five-minute rainfall data and a
30-minute threshold-exceedance prediction task.

## Report structure

1. **Data and computing environment.** Describe the source, timestamp
   alignment, cleaning, coverage definitions, and the Dataproc/Spark execution
   environment.
2. **Task 1: rainfall patterns.** Compare occurrence and conditional wet
   intensity within the year, hourly patterns, annual distributions, and
   station coverage. Interpret annual differences as observations of a
   changing network rather than a climate trend.
3. **Task 2: exceedance prediction.** Define the six-reading future target,
   predictor sets, temporal partitions, candidate selection, calibration,
   monotone probabilities, and 2024 evaluation.
4. **Discussion and conclusion.** Summarize the predictive contribution of
   same-station rainfall history and the limits of retrospective data,
   network coverage, and unseen-station evaluation.
5. **Appendix.** Include a Stage 21 view and an Executors view from the final
   GBT-B application on the eight-worker cluster.

## Current report artifacts

- Source: `report/main.tex`
- PDF: `report/main.pdf`
- Final figures: `report/figures/final/`
- Supporting tables: `report/tables/final/`

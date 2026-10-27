# README animation

`rainfall-pipeline.gif` is the looping README banner. The matching MP4 is a
higher-frame-rate copy for presentations or sharing, and the PNG is a still
preview. All three are 1280 x 720 pixels. The loop lasts eight seconds; the GIF
uses 12 frames per second, and the MP4 uses 24.

The motion illustrates the workflow. The island outline and sensor positions
are schematic; rain, pulses, and moving data packets are decorative. The
animation does not show live weather, observed storm movement, prediction
latency, or an active cloud cluster.

The numeric labels come from these tracked files:

- `config/experiment.json`: input years and the saved eight-worker compute
  profile, with eight two-core executors.
- `report/tables/final/final_evaluation_metrics.csv`: final PAVA and climatology
  PR-AUC, Brier score, and held-out base-window count at 0.5 mm, from run
  `20261016-v2-fixed8-04`. The displayed PR-AUC uses Spark's 1,000-bin evaluator.

Regenerate from the repository root with Python 3.10 or newer, Pillow, ffmpeg
with the libx264 encoder, and either Arial or DejaVu Sans installed:

```bash
python -m pip install Pillow
python scripts/render_readme_animation.py
```

The renderer reads only local configuration and saved metrics. It does not
execute notebooks, train models, or access cloud resources. Frames are written
to a temporary directory and removed after encoding. Font selection follows
the available system fonts, so exact pixels can differ between platforms.

To keep README media accessible, the embedding includes descriptive alt text
and a link to the still preview. The nearby README results table contains the
same evaluation metrics in text.

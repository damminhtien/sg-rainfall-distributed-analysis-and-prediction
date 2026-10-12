# DSA5208 Project 2 — TODO

Cập nhật ngày **12/10/2026**. Bài cá nhân: **DAM MINH TIEN — A0355091E**.
Mục tiêu: hoàn thành đúng Task 1 và Task 2 của đề, với báo cáo có bằng chứng thực tế.
Không cần đặt câu hỏi nghiên cứu hoặc tuyên bố novelty. Project 1 chỉ tham khảo hình thức.

Đọc [TASK_SPEC.md](TASK_SPEC.md) để xem toàn bộ công thức, quy tắc thống kê, acceptance criteria,
nguồn tham khảo và nhật ký chạy cũ. Checklist này là chỉ dẫn hiện hành; đặc tả là tài liệu chi tiết.

**Cách cập nhật:** `[x]` = có artifact local hoặc quyết định đã xác nhận; `[ ]` = còn việc phải làm,
kể cả code đã có nhưng chưa chạy hoặc chỉ chạy một phần. Chỉ đánh dấu hoàn tất khi ghi được evidence.
Một notebook không có error output chưa chứng minh tất cả cells/actions đã hoàn tất.
Các kết quả đã có bên dưới thuộc run cũ; cần giữ nguyên và đối chiếu khi retry.

## 1. Ràng buộc áp dụng xuyên suốt

- [x] Chốt tác giả cá nhân DAM MINH TIEN, Student ID A0355091E.
- [x] Chốt giả định timestamp là **cuối khoảng đo 5 phút** theo câu trả lời người dùng; không gọi là xác nhận từ NEA.
- [x] Ghi chỉ dẫn mới: **tiếp tục, không chờ đối soát cost**. Không yêu cầu người dùng gửi Reports/Cost table để mở bước tiếp theo.
- [ ] Khi thực hiện cloud, xác minh đúng project `sg-rainfall-dsa5208-261011`, region `asia-southeast1`, Billing Account for Education `01DAD8-AE5F1A-E3910A`.
- [ ] Chỉ dùng credit `3LU63BPGY77JFDQQ` trên account này; tuyệt đối không đổi account, redeem credit khác, thêm payment method hoặc dùng paid fallback. Chỉ dẫn bỏ chờ cost không mở rộng quyền này.
- [ ] Chạy toàn bộ code project qua **Jupyter Notebook trên Dataproc**, không dựng Spark cluster local, kể cả lệnh gcloud bằng cell `subprocess`; giữ stdout, bảng, hình và trạng thái trong output. Hàm tái sử dụng có thể nằm trong `src/` và `scripts/`.
- [ ] Với **mỗi action Spark**, sau trạng thái cuối chụp Jobs summary và **từng Job detail trên Spark Web UI**; dùng History Server khi app đã kết thúc. Xem mục 12.
- [ ] Mỗi run lưu RUN_ID, Git commit, input/config hashes; dùng prefix riêng, giữ artifacts thất bại, không overwrite final outputs.
- [ ] Giữ raw bất biến ngoài repo; không commit credential, JSON keys, `cloud.env`, raw/cache, workstation files hoặc skills.

Snapshot credit người dùng cung cấp ngày 12/10/2026: **Available, 96%, $47.91/$50.00**;
credit one-time, Any service on this billing account, 17/08/2026–14/08/2027, Net pricing.
Đây là snapshot, không phải số dư live hay bằng chứng project tiêu $2.09. Cost reconciliation có thể làm
sau, không còn là điều kiện chờ người dùng. CLI không gán Credit ID riêng cho từng job.

## 2. Việc tiếp theo — làm theo thứ tự

- [ ] **A. Đồng bộ notebook billing policy** với chỉ dẫn mới: bỏ điều kiện bắt buộc chờ Reports/Cost table và reserve cũ; vẫn kiểm tra đúng account/project/credit. Không điền số usage/pending giả để vượt gate. `src/rainfall/billing.py` và notebook cloud hiện còn policy cũ, chưa sửa trong lần chuyển TODO này.
- [ ] **B. Mở session Jupyter mới** sau preflight account/IAM/quota/network; pin runtime tương thích, bật event log và thử mở Spark UI trước full run.
- [ ] **C. Khép cleaning audit:** xuất các bảng còn thiếu và đối soát raw = retained + excluded trước khi coi cleaning hoàn tất.
- [ ] **D. Hoàn tất notebook 04** với attempt prefix mới: baseline, LR/GBT A/B, selection, calibration, frozen manifest. Không đánh dấu app5 dở dang là hoàn thành.
- [ ] **E. Chạy notebook 05** sau freeze: đánh giá 2024 một lần, unseen thresholds, uncertainty/error analysis; sau đó EDA đủ 2017–2024.
- [ ] **F. Thu đủ ảnh Spark UI, download/reload model**, hoàn thiện báo cáo, package và dọn tài nguyên.

## 3. Đề bài, nguồn dữ liệu và provenance

Chi tiết: TASK_SPEC mục 1–3. Deadline theo mô tả: **18/10/2026, 23:59**.

- [ ] Đối chiếu lại đề/Canvas: Task 1 within-year và across-year; Task 2 xác suất mưa 30 phút **hơn** ngưỡng; Spark structured processing, Spark MLlib; PDF/source/README/trained model.
- [ ] Xác nhận timezone deadline, giới hạn upload, định dạng và tên package trên Canvas.
- [x] Có `manifests/input_manifest.json`: đúng tám năm 2017–2024, SHA-256, bytes, rows, schema, dataset IDs/URLs và actual time ranges; không dùng 2016 làm warm-up.
- [x] Manifest ghi rõ retrieval time gốc unknown; không thay bằng ngày scan. Tổng inventory: 48,538,713 rows, 8,047,044,701 bytes; timestamp offsets +08:00.
- [ ] Khi retry, đối chiếu raw/GCS với manifest bằng byte hashes; không dùng object listing hoặc min/max timestamp để kết luận dữ liệu đầy đủ.
- [ ] Dùng paths portable qua DATA_ROOT/config; cloud đọc danh sách file từ manifest, không wildcard ngoài phạm vi.
- [ ] Lưu run provenance và source URL; kiểm tra notebook/package không phụ thuộc đường dẫn cá nhân.

## 4. Cloud, Dataproc và Jupyter

Chi tiết: TASK_SPEC mục 4–5; điều kiện chờ cost cũ đã được thay thế ở mục 1 trên.

- [ ] Kiểm tra billing linkage và `billingEnabled`, identity, region và resource targets trong notebook trước tạo tài nguyên.
- [ ] Kiểm tra CPU/disk quota, subnet/Private Google Access, firewall nội bộ và `iam.serviceAccounts.actAs`.
- [ ] Xác minh VM service account có Dataproc worker, GCS read/write và quyền đọc project billing cần thiết; không dùng JSON keys.
- [ ] Dùng bucket private/uniform access cùng region; giữ `raw/`, `manifests/`, `processed/`, `features/`, `notebooks/`, `models/`, `results/`, `staging/`, `temp/` có run prefixes.
- [ ] Chỉ upload tám CSV và manifest; đọc GCS, không copy toàn bộ raw sang mỗi worker.
- [ ] Dùng cấu hình đã chạy tương thích: 1 master + 2 workers `e2-standard-4`; master disk 100 GB, mỗi worker 75 GB; pin image `2.2.87-debian12` hoặc ghi rõ thay đổi đã kiểm tra.
- [ ] Ghi actual Spark/Python/Java/OS versions, executors, YARN master, machine/vCPU/RAM/disk, application ID và Spark config; timezone `Asia/Singapore`.
- [ ] Dùng Python/PySpark đi kèm image; không cài PySpark mới đè runtime. Import-check SciencePlots/Matplotlib bằng compatible wheels khi private network không có PyPI egress.
- [ ] Smoke qua notebook: explicit-schema CSV → Parquet write/read → MLlib → GCS; xác minh executors thật trên workers.
- [ ] Bật Jupyter Component Gateway, event logging/History Server và xác minh truy cập Spark Jobs/Job detail.
- [ ] Bật idle timeout và hard lifetime; checkpoint theo phase. Session cũ 45 phút idle/3 giờ hard-delete đã ngắt model run, nên phải hoàn tất/export phase trước hạn hoặc chia session.
- [ ] Persist notebooks có output lên GCS và export local sau từng phase; không chỉ giữ trên master disk.
- [ ] Tối ưu plan/skew/partitions/cache trước tăng compute; không collect raw/features về driver.

## 5. Cleaning và chất lượng dữ liệu

Chi tiết: TASK_SPEC mục 6. **Chưa hoàn tất**: cleaned `_SUCCESS` không thay thế audit.

- [ ] Explicit schema 12 cột; giữ source file/raw fields/malformed records; parse timestamp/offset/local date/year.
- [ ] Audit alignment theo trạm, đặc biệt 2017 `08:04:59`; freeze tolerance và quy tắc chuẩn hóa, không floor tùy tiện hoặc nhập hai intervals khác nhau.
- [ ] Audit station ID/name/device/coords, range/type/unit, metadata drift/relocation; rainfall finite ≥0, đơn vị mm và đúng reading type.
- [ ] Parse update fields; kiểm tra missing/invalid/update-before-observation, thứ tự revisions. Ghi proxy publication và giới hạn khi chưa xác minh semantics.
- [ ] Bảo đảm key station + interval_end unique: giữ một exact duplicate; quarantine conflicting records hoặc chọn version bằng policy có bằng chứng, không random dropDuplicates/cộng bản trùng.
- [ ] Không missing=0, future backfill/interpolation; không xóa mưa cực đoan thật nếu thiếu QC evidence/sensitivity.
- [ ] Đếm loại bỏ theo thứ tự không trùng; đối soát **raw = retained + excluded**, kiểm tra key uniqueness và mẫu records từng lý do.
- [ ] Coverage grid 288 intervals/ngày, leap years; tách fixed-calendar và active-period. First/last observation không phải commissioning/retirement.
- [ ] Xuất/lưu `rejected`, `quality_by_year`, `exclusion_counts`, `alignment_distribution`, `coverage_and_gaps`, metadata drift và audit config.
- [ ] Cleaned Parquet partition year (month nếu có lợi), tránh tiny files; lưu snapshot/manifest và runtime/action evidence.
- [ ] Đối chiếu lại các outputs downstream với cleaning policy sau khi audit khép kín; nếu policy đổi, regenerate features/EDA và ghi version.

Evidence local: `notebooks/executed/01_data_quality-attempt-2.ipynb` có hai cells đã chạy và không có
error output; đây là execution một phần, không đủ đánh dấu toàn phase. Nhật ký raw audit ở TASK_SPEC
ghi app1 jobs 0–20 thành công; cleaning audit còn thiếu các outputs trên.

## 6. Task 1 — EDA và phân tích patterns

Chi tiết công thức/mẫu số: TASK_SPEC mục 7 và 7.1. Mỗi analysis cần mục đích, cohort, denominator,
statistic, kết quả, diễn giải và giới hạn. Notebook 02 chỉ dùng 2017–2023 trước freeze;
EDA 2024 làm sau notebook 05/freeze, không dùng để chọn model.

- [x] Có pretest aggregates CSV và `findings.json` ở `report/tables/pretest/`, analysed_years 2017–2023; notebook `02_task1_weather_patterns-attempt-2.ipynb` có bốn cells đã chạy, không có error output.
- [x] Findings ghi common cohort rỗng ở cả cutoffs 90/95/99%; không có annual common-cohort comparison để tuyên bố xu hướng.
- [ ] Khép các kiểm tra bên dưới cho **đủ 2017–2024**, giữ pretest outputs riêng và đối chiếu với audit cleaning hoàn chỉnh.
- [ ] Coverage heatmap station/year và year/month: valid/expected intervals, stations, dates, denominators; không giấu periods zero-observation.
- [ ] Distribution mỗi năm: valid/dry/wet counts/fractions, valid mean, conditional wet mean/median/P90/P95/P99/max; ghi pooled observation weighting khác equal-station.
- [ ] Tail sensitivity: wet P99 từ train 2017–2022 cố định (pretest findings hiện 7.0 mm), strict exceedance và denominator valid; không gọi là warning threshold chính thức.
- [ ] Daily: chỉ complete days có 288 unique exact-grid intervals; equal station-month daily mean và support, so với incomplete coverage.
- [ ] Monthly: station-month coverage ≥95%; observed total/wet frequency/conditional intensity, equal station weighting và support. Không cộng các trạm thành “Singapore total”, không gọi partial total là full-month rainfall.
- [ ] Hourly SGT interval-end: month AND hour-group coverage ≥95%, expected 12 × days; heatmap equal station-year-month-hour và yearly curve equal station-month, có support.
- [ ] Across-year: common annual station cohort primary95%, sensitivity90/95/99%; nếu rỗng thì ghi N/A và trình bày changing-cohort descriptive riêng, không hạ cutoff sau khi xem kết quả để tạo trend.
- [ ] Spatial: time-stable coordinates, annual coverage ≥95%, station support/year averaging; không suy ra area interpolation từ station map.
- [ ] Wet spells: positive readings cùng station, cách đúng300s; duration5×n phút; tách spells censored bởi gap/boundary, median/P90/P99/max; không gọi là meteorological storm events.
- [ ] Collect aggregates ≤20,000 rows/table; SciencePlots PDF/PNG, units/axes/captions đọc được A4, ghi actual extrema/support trong findings JSON.
- [ ] Viết within-year và across-year findings từ bảng/hình thật; giải thích bias coverage/network, không claim climate trend hoặc causal monsoon explanation thiếu nguồn.

## 7. Task 2 — Target, features và leakage

Chi tiết: TASK_SPEC mục 8–9. `r(s,u)` thuộc `(u−5min,u]` theo giả định người dùng.
`R30(s,t) = sum(r(s,t+5k min), k=1..6)`; `Y = 1[R30 > threshold_mm]`.
Reading tại t không nằm trong label; đúng bằng threshold là class0.

- [x] Có notebook `03_task2_features-attempt-1.ipynb` với bốn cells đã chạy, không có error output. Đây là evidence feature run, không chứng minh model/test hoàn tất.
- [ ] Đối chiếu fixtures dry/wet/gap/duplicate/equality/off-grid và key uniqueness với config hiện hành; sáu future intervals distinct/contiguous/aligned, không chỉ count hoặc lead xuyên gap.
- [ ] Freeze window boundaries (future[300,1800], history[-1500,0] cho cửa sổ30 phút), elapsed-time lags, cross-year context; không mix station.
- [ ] Ghi eligibility/current-history missing policy và attrition train/selection/calibration/test; không thay missing future bằng0.
- [ ] Ghi retrospective event-time assumption nếu update/publication chưa xác minh; availability-aware mode lọc cả interval_end≤t và available_time≤t, không dùng later revisions.
- [ ] Kiểm tra fixtures sửa rainfall/update/metadata sau t không đổi features tại t; delayed observation bị loại trong availability-aware mode.
- [ ] Feature dictionary chỉ ghi features thực dùng: station/time-valid coords/unseen, fractional-hour và leap-year day-of-year cyclic, month, numeric threshold, rainfall lags/windows, valid counts/missingness/latest age.
- [ ] Lags5/10/15/30/60 phút, totals15/30/60/180 phút nếu thực dùng; boundaries/missing rules rõ. Fit imputer/indexer/scaler/feature selection trên train của fold.
- [ ] Build base station/time features một lần rồi expand thresholds0.2/0.5/1/2/5/10 mm; cùng base sample group thuộc cùng split/sampling; ghi base/expanded counts.
- [ ] Chốt numeric threshold domain từ train support; đánh giá unseen midpoints, xử lý out-of-domain bằng status rõ, không hứa extrapolation.
- [ ] Public inference nhận station_id/prediction_time/threshold_mm, build đúng features/preprocessing và trả probability[1]/status cho unseen/missing/domain.
- [ ] Khai báo sáu future sums2024 đã materialize trước freeze (chỉ valid counts được xem theo nhật ký); không mô tả quá trình là hoàn toàn blind.

## 8. Temporal split, tuning, models và freeze

Chi tiết: TASK_SPEC mục 10–11.

| Subset | Thời gian prediction | Công việc được phép |
|---|---|---|
| Train | 2017–2022 | Fit transforms/base models, train support/sampling |
| Selection | Jan–Jun 2023 | Chọn features/hyperparameters/cutoff/calibration recipe |
| Calibration | Jul–Dec 2023 | Fit calibrator sau base-model freeze |
| Final test | 2024 | Đánh giá frozen system một lần |

- [ ] Với split[start,end), giữ t≥start và t+30min<end; audit endpoints/purge, history trước start hợp lệ; không random CV.
- [ ] Chốt sampling pilot train/seed/fraction/actual year coverage trước threshold expansion; không gọi subsample là full-data training.
- [ ] Khóa tuning cap: tối đa 4  LR configs và4 GBT configs trên train pilot B; chọn bằng selection và dùng cùng settings/samples cho A/B.
- [ ] Fit climatology empirical survival station/month/hour; freeze smoothing/support và fallback sparse/unseen về nhóm rộng/global, lưu lookup.
- [ ] Fit MLlib LR PipelineModel gồm station indexing/one-hot/impute/assembly/scaling phù hợp; giữ sparse vectors, không center one-hot.
- [ ] Fit MLlib GBT PipelineModel, station categorical metadata/maxBins đủ cả unseen bucket; không scaling thừa.
- [ ] Giữ label0/1 và xuất probability[1], probabilities finite[0,1]. Natural prevalence mặc định; weights/oversampling nếu có phải khai báo, không rebalance test.
- [ ] A=station+time+threshold, B=A+history+missingness: cùng eligible samples/grid/tuning budget; báo ablation trên selection.
- [ ] Dùng attempt prefix chưa tồn tại (`models/attempt-02/` nếu còn trống); giữ partial app5 artifacts. Nếu đã có partial attempt thì đổi cả notebook04/05 sang attempt mới.
- [ ] Hoàn tất notebook04 và export model/selection/calibration outputs; `gbt_A/` tồn tại trong nhật ký cũ không chứng minh `gbt_B/` hoặc freeze đã hoàn tất.
- [ ] Inspect SQL/window/join/skew, reused cache/unpersist, rows/bytes/partitions/runtime và workers. Nếu chunk thời gian, dùng history halo180 phút/future30 phút rồi trim ownership.
- [ ] Freeze manifest gồm cleaning/alignment/availability, domain/grid/features/splits/sampling/seed, model/hyperparams/calibrator/cutoff/metrics/runtime/Git commit trước notebook05.
- [ ] Không refit base model trên2023 rồi giữ calibrator cũ; refit cần temporal out-of-fold calibration riêng. Changes sau test phải ghi post-test exploration.

## 9. Metrics, calibration và final evaluation

Chi tiết: TASK_SPEC mục 12. **Chưa có final-test metrics hoàn chỉnh.**

- [ ] Khóa selection objective mean PR-AUC primary thresholds đủ classes từ train support; tolerance/tie-break Brier rồi model đơn giản, undefined fallback rule trước test.
- [ ] Fit calibration recipe từ selection; calibrator từ holdout Jul–Dec 2023. Assessment bắt buộc, correction chỉ khi cần; lưu mapping/domain/fallback, không chọn bins/cutoff trên test.
- [ ] Sau frozen manifest, notebook05 kiểm tra config/model root và từ chối overwrite final-evaluation; đánh giá climatology/LR-B/GBT-B trên cùng eligible 2024 cohort.
- [ ] Per-threshold metrics: base/expanded count, positives/negatives/prevalence, PR-AUC/ROC-AUC/Brier/log loss và cutoff precision/recall/F1.
- [ ] Ghi Spark evaluator version/numBins; không gọi areaUnderPR là Average Precision; logloss epsilon frozen, AUC N/A nếu single-class, Brier skill chỉ khi denominator>0.
- [ ] Frozen macro weights; pooled-threshold metrics chỉ bổ sung. PR curves primary thresholds có prevalence reference/support, không giả định constant predictor area luôn bằng prevalence.
- [ ] Reliability diagram raw/calibrated: mean p/observed frequency/count mỗi frozen bin, cùng cohort.
- [ ] Monotonicity cùng station/time/history: threshold tăng thì probability không tăng; violation rate/max excess/tolerance raw và calibrated.
- [ ] Đánh giá unseen thresholds/domain và error strata station/month/threshold/unseen/onset/continuation định nghĩa bằng available history.
- [ ] Paired day/week block-bootstrap uncertainty: tất cả stations/thresholds cùng block, block length chọn train/validation, không bootstrap threshold copies độc lập. Ưu tiên Brier difference nếu hạn chế runtime, ghi metrics chỉ có point estimate.
- [ ] Lưu evaluation tables/curves/support/exclusions/run manifest; kết luận đúng kể cả ML không hơn baseline hoặc calibration không cải thiện.

## 10. Báo cáo — ưu tiên Task 1 và Task 2

Chi tiết: TASK_SPEC mục 13.1–13.6; sửa tại `report/main.tex`, giữ editor hiện tại.

- [ ] Áp dụng `data/academic-writing-SKILL.md` khi viết; tiếng Anh rõ ràng, A4 một cột/font serif/captions/cross-references; Project1 layout tái dựng từ PDF do chưa có .tex/.cls/.sty.
- [ ] Bìa cá nhân đúng tên/ID; 10–12 trang thân bài chỉ là gợi ý, không phải page limit/rubric chính thức.
- [ ] Introduction ngắn: data2017–2024, hai tasks và phạm vi, không thêm research question/novelty.
- [ ] Dataset/cleaning: source/schema/mm/SGT, interval-end assumption/update semantics, bảng yearly raw/retained/stations/validdays/coverage/exclusions; giải thích missing≠0/conflicts/gaps/revisions và sample attrition.
- [ ] Computing environment: actual image/versions/master/workers/vCPU/RAM/disks/executors/config, pipelineflow/timings và Spark worker evidence; không dùng planned config như thực đo.
- [ ] Task1 khoảng2.5–3 trang: coverage → distribution → monthly/hourly → across-year/cohort sensitivity; số liệu thật, denominator/support, mỗi đoạn quan sát → cách tính → diễn giải → giới hạn.
- [ ] Task1 spatial/tail/complete-day/wet-spell đưa appendix khi không bổ sung finding chính; không gọi mạng trạm changing-cohort là climate trend.
- [ ] Task2 khoảng3–4 trang: công thức/timeline6 future intervals/strict>, input-output/domain/missing/unseen và retrospective limitation.
- [ ] Bảng features thực dùng có source/window/availability/missing rule; split table có base/expanded/positives/prevalence/purge.
- [ ] Models: baseline/LR/GBT, actual selected hyperparameters, train/tuning/selection/calibration; phân biệt rainfall threshold với probability cutoff.
- [ ] Main results: ba models cùng2024cohort, PR/ROC/Brier và per-threshold support; logloss/cutoff metrics bổ sung, macro/pooled/N/A rõ.
- [ ] Probability quality: reliability counts/raw-calibrated, PR curves/prevalence, A/B ablation, errors/unseen/monotonicity và uncertainty phù hợp evidence.
- [ ] Discussion: kết nối patterns với features, rainfall-only/missing/revisions/rare heavy rainfall/record length/domain/unseen/availability limits; không overclaim real-time/robust/significant.
- [ ] Reproducibility: README/exact runtime/config/commit/manifest/modelpackage/loadpredict; conclusion trả lời cả tasks, không thêm kết quả mới.
- [ ] Viết abstract150–200 từ cuối cùng với ít nhất một finding thật mỗi task; cập nhật conclusion khi final results có, không giữ metric unavailable như bản nộp hoàn chỉnh.
- [ ] References nguồn thật, lecture title/page khi dùng; AI declaration đúng công cụ/model/công việc hỗ trợ và trách nhiệm cá nhân.
- [ ] Figure/table index nối source output/run/split/cohort/config/commit; tạo numeric macros từ frozen outputs, số abstract/bảng/conclusion khớp rounding.
- [ ] Caption tự hiểu được, axesunits/legend/A4readability, dẫn hình trong text; dùng `quality_by_year.csv`, coverage/monthly/hourly/annual figures, model/thresholdtables, PR/calibration và ablation/errors khi có evidence.
- [ ] Dùng built-in LaTeX compiler cho file hiện tại, sửa lỗi/references/overflow; render và đọc toàn bộ pages A4, không TODO/placeholder/brokenref trong PDF nộp.
- [ ] Requirement-to-section/page checklist đủ cleaning, within/acrossyear, features/models/environment/performance/limitations/AI; đối chiếu từng số với artifact.

## 11. Model package, README và bàn giao

Chi tiết: TASK_SPEC mục 13 và14.

- [ ] Download toàn bộ Spark PipelineModel directory tree, calibrator/climatologylookup/fallback, schema/config/domain/availability, manifest/versions/commit và public prediction entry point.
- [ ] Có offline history/metadata fixtures, expected probability/status/tolerance cho valid/unseen/missing/out-of-domain; không phụ thuộc bucket cá nhân còn tồn tại.
- [ ] Fresh session đúng runtime: load/infer từ public station/time/threshold contract và compare saved fixtures; save thành công chưa đủ.
- [ ] README đủ prerequisites/config/data source/manifest/upload/notebookorder/runtime/lifecycle/outputs/loadpredict; mọi lệnh thực thi project qua notebook.
- [ ] Restart-and-run-all notebook workflow phù hợp split/freeze, giữ outputs/provenance; không vô tình xem test khi rerun selection.
- [ ] Archive tự chứa README/requirements/config mẫu/notebooks/src/scripts/model/fixtures/report.pdf; giữ LaTeX/figures/aggregate tables để reproduce report.
- [ ] Loại credentials/raw/cache/temp/hardcoded personal paths; kiểm tra Canvas size limit, giải nén chính archive và load/infer lần cuối.
- [ ] Nộp PDF/source/model đúng yêu cầu trước deadline; lưu upload receipt. Chưa có receipt thì chưa đánh dấu submission hoàn tất.

## 12. Spark UI evidence cho mọi lần chạy

Chi tiết: TASK_SPEC mục5/13.5. Evidence root `report/evidence/spark-ui/<RUN_ID>/index.csv`.

- [x] Có index/API metadata và ảnh `20261011-full-01/app5-jobs-summary-active-checkpoint.jpg` kèm note. Đây là active checkpoint lưu từ trang cũ, **không chứng minh terminal status**.
- [ ] Chạy notebook11 cập nhật index một dòng/action: command/notebookcell, RUN_ID/ApplicationID/JobIDs/finalstatus/duration/stages/tasks, summarypath và mọi detailpaths.
- [ ] Sau action kết thúc, mở đúng Spark application, tab Jobs; nếu jobs còn chạy thì chờ terminal state và chụp lại.
- [ ] Chụp summary và mọi Job detail từ **Spark UI thật**, lưu tên có ApplicationID/JobID/status. Không thay bằng Dataproc Jobs/YARN/API JSON hoặc ảnh minh họa.
- [ ] Capture cả failed/cancelled actions; nếu không có JobID giữ summary và ghi nojobs. Nếu không khôi phục được event logs/UI thì ghi thiếu evidence, không tự điền trạng thái.
- [ ] Bổ sung app1/app3/app4/app5 detail/summary còn thiếu bằng History Server nếu logs khôi phục được; checkpoint app5 không thay thế các ảnh này.
- [ ] Đối chiếu index với notebook/log/UI, mọi JobID có ảnh; chèn ảnh/index vào report appendix để xem lại.
- [ ] Download evidence trước teardown; ghi missing screenshots trung thực nếu chưa hoàn thành.

## 13. Shutdown và kiểm tra hoàn thành

- [ ] Export notebooks, persist/download cleaned/features/models/results/manifests/evidence; xác minh local bản bàn giao trước xóa cloud artifacts cần dùng.
- [ ] Xóa đúng cluster qua notebook00, lưu output; truy vấn lại project/region cluster listing.
- [ ] Kiểm tra VM/disks/reservedaddresses/tài nguyên phụ; không suy ra đã cleanup chỉ từ notebook mất kết nối.
- [ ] Dọn GCS staging/temp/raw/outputs không còn cần sau download/check, giữ retention rõ; cluster deletion không xóa storage charges.
- [ ] Nếu đối soát chi phí sau run thì ghi snapshot/account-level/project-level/pending trung thực; không dùng dữ liệu chưa xác minh làm actual runtime cost trong report.
- [ ] Audit cuối: cleaning khép kín; Task1 đủ 8 năm; target/leakage/splits đúng; ba models/calibration/A–B/monotonicity/unseen/metrics có evidence; freeze trước test; ảnh từng job; notebookoutputs; freshsessionreload; PDF/package/receipt; cleanup.

## 14. Extensions — chỉ sau phần cốt lõi

- [ ] Nếu cần và còn thời gian: temporal folds đúng order/purge, availability-delay sensitivity chọn bằng train/validation.
- [ ] Optional C=B+neighbours: time-valid locations/as-offilter, không chọn correlation2024; samebudget/intersection comparison và coverage riêng.
- [ ] Optional monotonic projection/interpolation: freeze và lưu trong inference pipeline trước test, không chỉ sửa plot.
- [ ] Optional thêm rolling/spatial/scaling experiments chỉ khi giúp yêu cầu đề; không trì hoãn report/modelpackage để mở rộng scope.

Không đánh dấu toàn dự án hoàn thành vì TODO đã được viết. Các ô chưa tick là công việc thực thi còn lại;
nhật ký đầy đủ và các lần chạy lỗi/dở dang được giữ nguyên trong TASK_SPEC.

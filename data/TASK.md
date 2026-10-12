# DSA5208 Project 2 — TODO

Cập nhật ngày **12/10/2026**. Bài cá nhân: **DAM MINH TIEN — A0355091E**.
Mục tiêu: hoàn thành đúng Task 1 và Task 2 của đề, với báo cáo có bằng chứng thực tế.
Không cần đặt câu hỏi nghiên cứu hoặc tuyên bố novelty. Project 1 chỉ tham khảo hình thức.

TASK này gồm checklist và thiết kế chi tiết/tham số theo config, code hiện tại.
Đọc [TASK_SPEC.md](TASK_SPEC.md) để xem bản đặc tả gốc, nguồn tham khảo và nhật ký chạy cũ;
TASK là chỉ dẫn hiện hành khi chính sách trong nhật ký đã được thay thế.

**Cách cập nhật:** `[x]` = có artifact local hoặc quyết định đã xác nhận; `[ ]` = còn việc phải làm,
kể cả code đã có nhưng chưa chạy hoặc chỉ chạy một phần. Chỉ đánh dấu hoàn tất khi ghi được evidence.
Một notebook không có error output chưa chứng minh tất cả cells/actions đã hoàn tất.
Các kết quả đã có bên dưới thuộc run cũ; cần giữ nguyên và đối chiếu khi retry.

## 1. Ràng buộc áp dụng xuyên suốt

- [x] Chốt tác giả cá nhân DAM MINH TIEN, Student ID A0355091E.
- [x] Chốt giả định timestamp là **cuối khoảng đo 5 phút** theo câu trả lời người dùng; không gọi là xác nhận từ NEA.
- [x] Ghi chỉ dẫn mới: **tiếp tục, không chờ đối soát cost**. Không yêu cầu người dùng gửi Reports/Cost table để mở bước tiếp theo.
- [x] Ngày 12/10/2026 xác minh live bằng `gcloud`: project `sg-rainfall-dsa5208-261011` ACTIVE, region `asia-southeast1`, billing enabled và link tới Billing Account for Education `01DAD8-AE5F1A-E3910A`. Kiểm tra lại trước từng cloud action.
- [ ] Chỉ dùng credit `3LU63BPGY77JFDQQ` trên account này; tuyệt đối không đổi account, redeem credit khác, thêm payment method hoặc dùng paid fallback. Chỉ dẫn bỏ chờ cost không mở rộng quyền này.
- [ ] Chạy toàn bộ code project qua **Jupyter Notebook**; notebook00 có thể chạy trên Mac để tạo cluster, Spark processing/training chạy trên Dataproc, không dựng Spark cluster local. Lệnh gcloud gọi bằng cell `subprocess`; giữ stdout, bảng, hình và trạng thái trong output. Hàm tái sử dụng có thể nằm trong `src/` và `scripts/`.
- [ ] Với **mỗi action Spark**, sau trạng thái cuối chụp Jobs summary và **từng Job detail trên Spark Web UI**; dùng History Server khi app đã kết thúc. Xem mục 12.
- [ ] Mỗi run lưu RUN_ID, Git commit, input/config hashes; dùng prefix riêng, giữ artifacts thất bại, không overwrite final outputs.
- [ ] Giữ raw bất biến ngoài repo; không commit credential, JSON keys, `cloud.env`, raw/cache, workstation files hoặc skills.

Snapshot credit người dùng cung cấp ngày 12/10/2026: **Available, 96%, $47.91/$50.00**;
credit one-time, Any service on this billing account, 17/08/2026–14/08/2027, Net pricing.
Đây là snapshot, không phải số dư live hay bằng chứng project tiêu $2.09. Cost reconciliation có thể làm
sau, không còn là điều kiện chờ người dùng. CLI không gán Credit ID riêng cho từng job.

### 1.1. Quy ước thiết kế và quản lý trạng thái

Tài liệu phân biệt ba loại thông tin: **đang cấu hình** là giá trị đọc từ repository ngày
12/10/2026; **đã chạy** phải có output/run evidence; **đề xuất** là thiết kế cần triển khai.
Không tự chuyển một tham số đang cấu hình thành hyperparameter đã được selection chọn.

- [ ] Dùng `config/experiment.json` làm nguồn tham số thống kê; lưu bản sao config cùng mỗi run.
- [ ] Các tham số infrastructure nằm trong notebook00; Spark session trong notebook04/05 phải được in ra và đối chiếu actual config, vì `getOrCreate()` có thể tái sử dụng session cũ.
- [ ] Tách `RUN_ID` cho input/cleaning/features và `MODEL_ATTEMPT` cho retry huấn luyện; ghi rõ model attempt đọc feature run nào. Không đổi RUN_ID làm mất nguồn gốc artifacts đã có.
- [ ] Trước mỗi phase, notebook hiển thị input URI, expected config hash, output URI và trạng thái output đã tồn tại; chỉ bắt đầu khi dependency hoàn tất.
- [ ] Khi phase thất bại, giữ exception/output/action IDs và artifacts đã ghi; ghi `incomplete` nếu mất connection trước khi biết terminal state.

**Điều kiện hoàn tất một phase:** đủ artifacts bắt buộc, số dòng/keys kiểm tra được,
notebook có output, metadata khớp input/config và ảnh UI của các Spark jobs đã tạo.
Không tick chỉ vì có `_SUCCESS` ở một thư mục con. Những mục acceptance trong TASK_SPEC
vẫn áp dụng nếu không bị chỉ dẫn mới của người dùng thay thế.

## 2. Việc tiếp theo — làm theo thứ tự

- [x] **A. Đồng bộ notebook billing policy**: đã bỏ Reports/Cost-table và reserve gate; notebook vẫn xác minh live project/billing-account linkage, runtime ghi đúng Credit ID được phép, và các notebook cloud dùng run-scoped runtime/source URI. Không tạo usage/pending giả. Credit áp dụng tự động ở account scope; `gcloud` không thể pin credit ID theo job.
- [ ] **B. Mở session Jupyter mới** sau preflight account/IAM/quota/network; pin runtime tương thích, bật event log và thử mở Spark UI trước full run.
- [ ] **C. Khép cleaning audit:** xuất các bảng còn thiếu và đối soát raw = retained + excluded trước khi coi cleaning hoàn tất.
- [ ] **D. Hoàn tất notebook 04** với attempt prefix mới: baseline, LR/GBT A/B, selection, calibration, frozen manifest. Không đánh dấu app5 dở dang là hoàn thành.
- [ ] **E. Chạy notebook 05** sau freeze: đánh giá 2024 một lần, unseen thresholds, uncertainty/error analysis; sau đó EDA đủ 2017–2024.
- [ ] **F. Thu đủ ảnh Spark UI, download/reload model**, hoàn thiện báo cáo, package và dọn tài nguyên.

### 2.1. Thiết kế workflow và dependency

| Notebook | Vai trò | Input chính | Output/điều kiện chuyển bước |
|---|---|---|---|
| `00_environment` | Account, upload, cluster lifecycle | Manifest + source/config bundle | Runtime manifest, Jupyter/GCS hoạt động |
| `01_data_quality` | Hash audit, cleaning, coverage | Tám CSV từ manifest | Cleaned Parquet + audit reconciliation |
| `02_task1_weather_patterns` | EDA trước test | Cleaned + coverage, 2017–2023 | Aggregates/figures/findings pretest |
| `03_task2_features` | History/label/splits | Cleaned exact-grid keys | Base features + eligibility/attrition |
| `04_task2_models` | Tuning, A/B, calibration, freeze | Base features train/selection/calibration | Models + frozen manifest |
| `05_final_evaluation` | Test, full EDA, package | Frozen models + base features/cleaned | Metrics2024, EDA2017–2024, deployment |
| `11_spark_ui_evidence` | Thu và đối chiếu evidence | Action logs/event logs/UI | Index + summary/detail screenshots |

Notebook07–10 là hỗ trợ execution/diagnostics/source/runtime; không thay đổi temporal protocol.
Notebook11 chạy song song việc review UI sau mỗi action, không đợi tất cả jobs xong mới bắt đầu thu ảnh.

- [ ] Resume bằng artifacts đã đối chiếu schema/config/hash thay vì chạy lại toàn bộ CSV scan theo thói quen.
- [ ] Nếu cleaning thay đổi dữ liệu, chạy lại notebook02/03 và huấn luyện từ attempt mới; nếu chỉ sửa caption thì không cần chạy Spark.
- [ ] Chia model workflow thành checkpoint đủ nhỏ để lưu candidate/fit/calibration progress trước hard lifetime; đây là việc cần sửa, notebook04 hiện gói nhiều bước trong một action lớn.
- [ ] Với retry sau freeze/test, phân biệt retry kỹ thuật trên cùng frozen design với đổi thiết kế; báo cáo mọi test access, không dùng lần chạy lại để chọn kết quả đẹp hơn.

## 3. Đề bài, nguồn dữ liệu và provenance

Chi tiết: TASK_SPEC mục 1–3. Deadline theo mô tả: **18/10/2026, 23:59**.

- [ ] Đối chiếu lại đề/Canvas: Task 1 within-year và across-year; Task 2 xác suất mưa 30 phút **hơn** ngưỡng; Spark structured processing, Spark MLlib; PDF/source/README/trained model.
- [ ] Xác nhận timezone deadline, giới hạn upload, định dạng và tên package trên Canvas.
- [x] Có `manifests/input_manifest.json`: đúng tám năm 2017–2024, SHA-256, bytes, rows, schema, dataset IDs/URLs và actual time ranges; không dùng 2016 làm warm-up.
- [x] Manifest ghi rõ retrieval time gốc unknown; không thay bằng ngày scan. Tổng inventory: 48,538,713 rows, 8,047,044,701 bytes; timestamp offsets +08:00.
- [ ] Khi retry, đối chiếu raw/GCS với manifest bằng byte hashes; không dùng object listing hoặc min/max timestamp để kết luận dữ liệu đầy đủ.
- [ ] Require `DATA_ROOT` for Notebook 00 and keep config paths project-relative; cloud reads the explicit manifest file list.
- [ ] Lưu run provenance và source URL; kiểm tra notebook/package không phụ thuộc đường dẫn cá nhân.

### 3.1. Data contract và hồ sơ provenance

Manifest là hợp đồng lựa chọn input, không phải bằng chứng dữ liệu đã sạch.
Mỗi file cần một record `year`, `relative_path`, `size_bytes`, `row_count`, `sha256`,
`schema`, `dataset_id`, `dataset_url`, timestamp min/max/offset và parse-error counts.
`retrieval_time=null` được giữ nguyên khi không biết thời gian download gốc.

- [ ] Validate đúng một file/năm theo tên `raw/HistoricalRainfallacrossSingaporeYYYY.csv`; số records trong manifest bằng 8, years bằng 2017…2024.
- [ ] Raw đọc dưới dạng chuỗi trước khi parse, để còn phân biệt dữ liệu gốc với null sinh ra do cast lỗi. Giữ `_source_year`, `_source_file`, `_corrupt_record`.
- [ ] Ghi SHA-256 của chính bytes manifest và source bundle; metadata object không thay thế byte hash nội dung CSV.
- [ ] Runtime record cần project/account/credit/region/bucket, run ID, Git commit, manifest/config/source-bundle hashes, creation timestamp và artifact roots.
- [ ] Kiểm tra source bundle chỉ gồm explicit allowlist code/config/notebooks/manifest; không zip toàn workspace có `.env`, raw hoặc browser credentials.

**Khóa dữ liệu:** raw observation key sau cleaning là `(station_id, epoch)`; epoch là UTC seconds,
nhưng year/month/hour/local_date được tính trong Asia/Singapore. Không trộn timestamp local-naive
với timezone-aware. Trạm là đơn vị quan sát; không tự xem một reading là đại diện diện tích Singapore.
Input record thiếu date hoặc date không khớp observation timestamp phải xuất audit reason, không sửa âm thầm.

**Đầu ra cần lưu:** `input_manifest.json`, runtime metadata, input hash audit và bảng inventory tám năm.
Trong report, đưa bảng tóm tắt năm/rows/stations/coverage; manifest chi tiết để ở package/repo.

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

### 4.1. Tham số infrastructure và Spark hiện có

| Tham số | Giá trị hiện trong notebook/config | Ý nghĩa và việc cần xác minh |
|---|---|---|
| Cluster | `dsa5208-rainfall` | Describe trước create/delete, tránh nhầm run khác |
| Bucket | `sg-rainfall-dsa5208-261011-data` | Private, cùng region, paths dưới `runs/<RUN_ID>/` |
| Machine | Master +2 workers `e2-standard-4` | Mỗi VM 4 vCPU/16 GB; tổng 12 vCPU không đồng nghĩa 12 cores cho executors |
| Disks | `pd-balanced`, master 100 GB, worker 75 GB | Tổng 250 GB boot disks; không quay lại 300 GB khi quota chưa đủ |
| Image/network | `2.2.87-debian12`, `--no-address` | Pin image; subnet/private access/IAP đúng |
| Lifecycle | `--delete-max-idle=45m`, `--delete-max-age=6h` | Giới hạn tuổi cluster; xuất checkpoint giữa các phase và xác minh auto-delete không cắt action |
| Driver/executor memory | `4g` / `4g` | Giá trị request, không gồm toàn bộ overhead/YARN reservation |
| Executors |4 instances ×2 cores; dynamic allocation=false | Mục tiêu 8 executor cores trên 2 workers; xác minh allocated executors |
| SQL shuffle | `shuffle_partitions=64` | Giá trị khởi đầu, chưa chứng minh tối ưu |
| Spark UI retention | Jobs/Stages/SQL executions mỗi loại 10,000 | Giữ job metadata nhiều bước; theo dõi driver/listener memory |
| SQL timezone/ANSI | `Asia/Singapore` / `false` | Cast lỗi có thể thành null nên phải audit null |
| Plot dependency | SciencePlots wheel 2.2.2 | Không cài toàn stack khác đè Python/PySpark của image |

- [ ] Describe actual cluster và SparkConf trong output; nếu tài nguyên allocated khác request, dùng actual values trong báo cáo.
- [ ] Event logs ghi `gs://<BUCKET>/spark-history/`; kiểm tra service account đọc được log trước khi teardown.
- [ ] Dùng `StorageLevel.DISK_ONLY` cho train/selection/scores khi RAM không đủ; chỉ cache dữ liệu tái sử dụng, unpersist trong `finally`.
- [ ] Profiling một phase: ghi input/output rows, partitions, wall time, shuffle read/write, spill và task-duration spread; dùng để quyết định repartition, không tăng máy theo cảm tính.
- [ ] Checkpoint source bundle/runtime manifest trước session; download notebooks/evidence sau action dài và trước expiration.

**Lưu ý:** giữ 10,000 jobs không bảo đảm UI không OOM. Session mới nên có action boundaries rõ,
đóng application khi phase xong; mọi thay đổi memory/executors/shuffle phải có config hash/run note.

## 5. Cleaning và chất lượng dữ liệu

Chi tiết: TASK_SPEC mục 6. **Chưa hoàn tất**: cleaned `_SUCCESS` không thay thế audit.

- [ ] Explicit schema 12 cột; giữ source file/raw fields/malformed records; parse timestamp/offset/local date/year.
- [ ] Audit alignment theo trạm, đặc biệt 2017 `08:04:59`; freeze tolerance và quy tắc chuẩn hóa, không floor tùy tiện hoặc nhập hai intervals khác nhau.
- [ ] Audit station ID/name/device/coords, range/type/unit, metadata drift/relocation; rainfall finite ≥ 0, đơn vị mm và đúng reading type.
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

### 5.1. Quy tắc cleaning cụ thể và output schema

`alignment_tolerance_seconds=0`: code hiện **chỉ giữ exact grid** `epoch % 300 ==0`.
Record 08:04:59 bị loại theo off-grid policy hiện tại; không đổi thành 08:05 mà thiếu bằng chứng.
Nếu đề xuất tolerance khác 0 thì phải sửa validator/cleaner, chạy sensitivity và regenerate downstream.

| Bước | Policy đang có | Audit cần lưu |
|---|---|---|
| Parse/provenance | Explicit 12 fields + corrupt-record/source fields | Parser errors, missing station, timestamp/date/year mismatch |
| Value contract | Finite rainfall≥ 0, unit `mm`, reading type đúng config | Invalid/negative/type/unit counts |
| Coordinates | Finite lon[-180,180], lat[-90,90] | Invalid/range counts, station-coordinate changes |
| Alignment | Offset `pmod(epoch,300)` bằng 0 | Distribution offset trước loại bỏ theo năm/trạm |
| Precision | Scale1,000,000; round sang integer units | Reject nếu sai khác sau round vượt1e-10mm |
| Exact duplicate | So tất cả12 raw fields và source year; giữ1 | `exact_duplicate` count |
| Conflicting key | Cùng station/epoch khác rain/name/device/coords | Quarantine tất cả variants, không chọn rainfall tùy ý |
| Same measurement | Không conflict nhưng nhiều update records | Giữ1 theo update/read-update/source order, xuất duplicate count |

Code giữ **reason đầu tiên** cho mỗi raw row để exclusion totals không cộng trùng.
Tách `exact_duplicate`, `conflicting_key`, `same_measurement_duplicate` khỏi các invalid reasons.
Có thể audit thêm mọi flags cho diagnosis, nhưng bảng reconciliation phải dùng disjoint reasons.

- [ ] Clean output tối thiểu: station_id, epoch, observation_time, rain_mm/rain_units, year/local_date/month/hour, longitude/latitude, update_time/reading_update_time, station metadata/source.
- [ ] `quality_by_year`: raw_rows, retained_rows, rejected_rows, stations, valid station-days; join đủ tám năm, ghi 0 count khi không còn row.
- [ ] `exclusion_counts`: year/reason/count; `alignment_distribution`: year/station/offset/count; `coverage_and_gaps`: station/period/valid/expected/coverage/gap lengths.
- [ ] Kiểm tra raw_rows = retained_rows + sum(exclusion_counts) cho **từng năm** và toàn bộ; retained countDistinct(station,epoch) bằng retained rows.
- [ ] Update fields chưa được xác nhận publication: audit delay quantiles/revisions nhưng không biến chúng thành as-of truth. Metadata changes cần kiểm tra riêng, không tự gắn tọa độ cuối2024 cho tất cả quá khứ.

**Hoàn tất khi:** output đủ, reconciliation bằng 0 sai lệch, quality policy được ghi trong metadata
và downstream dùng đúng clean snapshot. Precision1e-6mm là quy tắc biểu diễn số, không phải độ chính xác sensor.

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
- [ ] Monthly: station-month coverage ≥ 95%; observed total/wet frequency/conditional intensity, equal station weighting và support. Không cộng các trạm thành “Singapore total”, không gọi partial total là full-month rainfall.
- [ ] Hourly SGT interval-end: month AND hour-group coverage ≥ 95%, expected 12 × days; heatmap equal station-year-month-hour và yearly curve equal station-month, có support.
- [ ] Across-year: common annual station cohort primary 95%, sensitivity 90/95/99%; nếu rỗng thì ghi N/A và trình bày changing-cohort descriptive riêng, không hạ cutoff sau khi xem kết quả để tạo trend.
- [ ] Spatial: time-stable coordinates, annual coverage ≥ 95%, station support/year averaging; không suy ra area interpolation từ station map.
- [ ] Wet spells: positive readings cùng station, cách đúng 300 s; duration 5×n phút; tách spells censored bởi gap/boundary, median/P90/P99/max; không gọi là meteorological storm events.
- [ ] Collect aggregates ≤ 20,000 rows/table; SciencePlots PDF/PNG, units/axes/captions đọc được A4, ghi actual extrema/support trong findings JSON.
- [ ] Viết within-year và across-year findings từ bảng/hình thật; giải thích bias coverage/network, không claim climate trend hoặc causal monsoon explanation thiếu nguồn.

### 6.1. Thiết kế tính EDA, mẫu số và artifacts

`wet_cutoff_mm=0`: wet khi `rain_mm>0`, dry khi bằng 0, invalid/missing không nằm trong mẫu số valid.
Coverage tháng = unique valid slots / `(days_in_month×288)`; coverage năm dùng365/366×288.
Coverage complete-day cần288 exact-grid keys, không dùng chỉ count raw rows trước dedup.

| Analysis | Statistic và mẫu số | Cách aggregate/report |
|---|---|---|
| Distribution | Wet/valid, dry/valid; conditional quantiles trên wet | Pooled reading weighting; dry mass và wet tail trình bày riêng |
| Monthly amount | Sum rainfall trên observed intervals tại station-month đủ coverage | Mean qua eligible stations, kèm số stations; chưa bù phần missing |
| Monthly frequency | Wet slots / valid slots tại station-month | Equal-station mean, không để trạm nhiều rows áp đảo |
| Hourly frequency | Wet/valid theo station-year-month-hour | Group coverage đủ95%, expected12×calendar days; mean eligible groups |
| Conditional intensity | Wet rainfall sum / wet slots | N/A khi không có wet slots, không dùng0 giả |
| Annual comparison | Các trạm đủcoverage ở **mọi năm** | Primary95%; sensitivity 90/95/99; rỗng thì N/A |
| Complete days | Daily sum/mean trên complete 288-slot days | Station-month statistics rồi equal-station mean, ghi số days |
| Wet spells | Consecutive positive readings đúng 300 s | Duration5n phút, boundary/gap-censored flag; không gộp qua gap |

`coverage_cutoffs=[0.90,0.95,0.99]`, primary0.95. Khi cohort rỗng, annual descriptive
có thể giữ để mô tả dataset nhưng phải ghi network composition khác nhau; không có controlled annual comparison.
Zero-support month/hour hiển thị thiếu dữ liệu; không fill0 rainfall trong figure.

- [ ] Pretest viết ở `report/tables/pretest/`; final đủ8 năm viết prefix `final/` và giữ `analysed_years` trong findings, không overwrite rồi mất khác biệt phạm vi.
- [ ] Nhóm outputs: coverage_summary/hourly_coverage; distribution_by_year/rainfall_histogram; station_month/monthly/hourly/hourly_by_year; annual_cohort_support/annual_common_cohort/annual_descriptive; daily_completeness/complete_daily_patterns; wet_spell_summary; station_map/station_spatial_summary.
- [ ] Mỗi CSV/JSON ghi year/group/statistic/denominator/support; findings lưu extrema có units, eligible stations và periods represented, không chỉ value.
- [ ] Hình chính: coverage context, distribution, monthly/hourly, annual cohort sensitivity. Spatial/tail/spells appendix nếu thân bài quá dài.
- [ ] Với train wetP99=7mm hiện có, nêu `rain_mm>7` strict exceedance; reference giữ từ2017–2022 và không recompute bằng2024 để thay threshold.
- [ ] Đối chiếu findings với aggregates sau rounding; các kết quả như hourly peak chỉ mô tả eligible cohorts, không suy ra coverage thấp không ảnh hưởng.

## 7. Task 2 — Target, features và leakage

Chi tiết: TASK_SPEC mục 8–9. `r(s,u)` thuộc `(u−5min,u]` theo giả định người dùng.
`R30(s,t) = sum(r(s,t+5k min), k=1..6)`; `Y = 1[R30 > threshold_mm]`.
Reading tại t không nằm trong label; đúng bằng threshold là class 0.

- [x] Có notebook `03_task2_features-attempt-1.ipynb` với bốn cells đã chạy, không có error output. Đây là evidence feature run, không chứng minh model/test hoàn tất.
- [ ] Đối chiếu fixtures dry/wet/gap/duplicate/equality/off-grid và key uniqueness với config hiện hành; sáu future intervals distinct/contiguous/aligned, không chỉ count hoặc lead xuyên gap.
- [ ] Freeze window boundaries (future[300,1800], history[-1500,0] cho cửa sổ 30 phút), elapsed-time lags, cross-year context; không mix station.
- [ ] Ghi eligibility/current-history missing policy và attrition train/selection/calibration/test; không thay missing future bằng 0.
- [ ] Ghi retrospective event-time assumption nếu update/publication chưa xác minh; availability-aware mode lọc cả interval_end≤t và available_time≤t, không dùng later revisions.
- [ ] Kiểm tra fixtures sửa rainfall/update/metadata sau t không đổi features tại t; delayed observation bị loại trong availability-aware mode.
- [ ] Feature dictionary liệt kê đúng các cột A/B thực đưa vào pipeline; current-missing and latest-age columns are not fitted or emitted as features.
- [ ] Lags 5/10/15/30/60 phút, totals 15/30/60/180 phút nếu thực dùng; boundaries/missing rules rõ. Fit imputer/indexer/scaler/feature selection trên train của fold.
- [ ] Build base station/time features một lần rồi expand thresholds 0.2/0.5/1/2/5/10 mm; cùng base sample group thuộc cùng split/sampling; ghi base/expanded counts.
- [ ] Chốt numeric threshold domain từ train support; đánh giá unseen midpoints, xử lý out-of-domain bằng status rõ, không hứa extrapolation.
- [ ] Public inference nhận station_id/prediction_time/threshold_mm; chỉ chấm điểm khi có current observation không null, và trả `missing_current_observation` không probability nếu thiếu.
- [ ] Khai báo sáu future sums 2024 đã materialize trước freeze (chỉ valid counts được xem theo nhật ký); không mô tả quá trình là hoàn toàn blind.

### 7.1. Tham số target và feature design đang triển khai

| Config/feature | Giá trị hiện hành | Thiết kế |
|---|---|---|
| `interval_seconds` |300 | Prediction t trên lưới5 phút SGT |
| Label horizon | `(t,t+1800 s]` | Sáu readings t+300, …,t+1800; đủ unique keys và endpoints |
| `rainfall_integer_scale` |1,000,000 | Cộng integer rainfall và so strict> integer threshold |
| Threshold grid |0.2,0.5,1,2,5,10mm | Train queries; mỗi base row có6 copies |
| Unseen grid |0.3,0.75,1.5,3.5,7.5mm | Midpoint queries đánh giá generalization trong domain |
| Threshold domain |[0.2,10]mm | Configured domain; còn cần chứng minh train support, không trả probability ngoài domain |
| Lags |5,10,15,30,60phút | Exact elapsed-time lookup, lag missing flag |
| Rolling windows |15,30,60,180 phút | Range `[-(W−5)×60,0]`, expected W/5 readings |
| Availability |`retrospective_event_time` | Final archived observations giả định available tại event time |

Ví dụ W=30 dùng t−25,t−20,…,t; future dùng t+5, …, t+30. Missing history vẫn
để null/partial counts và train-fitted imputation; missing future làm label invalid.
`sum_W` là tổng observed history, không phải tổng đã nội suy thành đủW phút.

**VariantA hiện có:** station one-hot + hour_sin/hour_cos/day_sin/day_cos/threshold_mm.
**VariantB:** A + rain_current +5 lags và5 missing flags +4 windows ×5 statistics
(sum/max/wet_count/count/coverage). Tổng numeric features A=5, B=36, ngoài station vector.
Month/hour có trong frame để baseline/error grouping; coords/month/latest age/current_missing
chưa nằm trong `feature_columns()` của ML pipeline. Không liệt kê chúng như features đã fit.

Hour cyclic dùng fractional hour/24; day cyclic dùng day-of-year fraction chia số ngày thực
365/366. Training and inference both require a current reading, so missingness/age are excluded.
Inference cũng chỉ hỗ trợ prediction time có current reading không null. Khi thiếu reading, API
trả status `missing_current_observation` với probability null; không tạo query row để impute
`rain_current`. `current_missing` và `latest_reading_age_minutes` không được tạo hoặc fit.

- [ ] Fixtures cụ thể: sáu dry future→R30=0; một wet future; thiếu 1 future→invalid; duplicate key phải bị xử lý upstream; R30=tau→0; t off-grid→reject; sửa future không đổi history features.
- [ ] Audit label_count/min/max chỉ đủ chứng minh contiguous khi cleaner đã bảo đảm unique exact-grid keys; không áp dụng shortcut này cho raw chưa dedup.
- [ ] Availability-aware/delay sensitivity hiện chưa có: config validator chỉ cho retrospective. Nếu triển khai extension, giữ mode mới riêng và chứng minh không dùng revisions sau t.

## 8. Temporal split, tuning, models và freeze

Chi tiết: TASK_SPEC mục 10–11.

| Subset | Thời gian prediction | Công việc được phép |
|---|---|---|
| Train | 2017–2022 | Fit transforms/base models, train support/sampling |
| Selection | Jan–Jun 2023 | Chọn model/hyperparameters theo PR-AUC/Brier và đánh giá nhu cầu calibration |
| Calibration | Jul–Dec 2023 | Fit calibrator sau base-model freeze |
| Final test | 2024 | Đánh giá frozen system một lần |

- [ ] Với split[start,end), giữ t≥start và t+30min<end; audit endpoints/purge, history trước start hợp lệ; không random CV.
- [ ] Chốt sampling pilot train/seed/fraction/actual year coverage trước threshold expansion; không gọi subsample là full-data training.
- [ ] Khóa tuning cap: tối đa 4 LR configs và 4 GBT configs trên train pilot B; chọn bằng selection và dùng cùng settings/samples cho A/B.
- [ ] Fit climatology empirical survival station/month/hour; freeze smoothing/support và fallback sparse/unseen về nhóm rộng/global, lưu lookup.
- [ ] Fit MLlib LR PipelineModel gồm station indexing/one-hot/impute/assembly/scaling phù hợp; giữ sparse vectors, không center one-hot.
- [ ] Fit MLlib GBT PipelineModel dùng station one-hot như code hiện có; nếu đổi sang indexed categorical thì kiểm tra metadata/maxBins đủ cả unseen bucket; không scaling thừa.
- [ ] Giữ label 0/1 và xuất probability[1], probabilities finite trong [0,1]. Natural prevalence mặc định; weights/oversampling nếu có phải khai báo, không rebalance test.
- [ ] A=station+time+threshold, B=A+history+missingness: cùng eligible samples/grid/tuning budget; báo ablation trên selection.
- [ ] Dùng attempt prefix chưa tồn tại; run mới hiện đặt `RUN_ID=20261012-full-01`, `MODEL_ATTEMPT=attempt-01` sau khi kiểm tra prefix local/GCS trống. Giữ partial app5 artifacts của run cũ `20261011-full-01`; nếu retry lần nữa thì đổi cả notebook04/05 sang attempt mới.
- [ ] Hoàn tất notebook04 và export model/selection/calibration outputs; `gbt_A/` tồn tại trong nhật ký cũ không chứng minh `gbt_B/` hoặc freeze đã hoàn tất.
- [ ] Inspect SQL/window/join/skew, reused cache/unpersist, rows/bytes/partitions/runtime và workers. Nếu chunk thời gian, dùng history halo 180 phút/future 30 phút rồi trim ownership.
- [ ] Freeze manifest gồm cleaning/alignment/availability, domain/grid/features/splits/sampling/seed, model/hyperparams/calibrator, fixed 0.5 reference cutoff, metrics/input-manifest hash và code commit nếu có trước notebook05.
- [ ] Không refit base model trên 2023 rồi giữ calibrator cũ; refit cần temporal out-of-fold calibration riêng. Changes sau test phải ghi post-test exploration.

### 8.1. Sampling, model parameters và trình tự lựa chọn

Các giá trị sau là **config/code đang có**, chưa phải selected result của attempt tiếp theo.
Boundary epochs lấy từ timezone-aware timestamps; purge dùng `epoch+1800<end`, kể cả cuối2024.

| Tham số | Giá trị | Cách dùng |
|---|---|---|
| `pilot_fraction` |0.005 (0.5%) | Tuning LR/GBT trên deterministic train pilot variant B |
| `final_train_fraction` |0.02 (2%) | Fit LR/GBT finalistsA/B; report đúng là subsampled train |
| `sampling_seed` |5208 | Hash station/epoch/seed; GBT random seed và bootstrap RNG |
| `primary_threshold_min_positives` |100 | Train support ≥100 positives và có negatives; không loại dựa trên test |
| LR `regParam` |0.001,0.01,0.1,1.0 |4 pilot candidates, L2 (`elasticNetParam=0`) |
| LR `maxIter` |50 | Giới hạn optimizer; ghi convergence actual nếu có |
| GBT `(maxDepth,maxIter)` |(3,20),(4,20),(3,40),(4,40) |4 pilot candidates, `maxBins=64`, seed 5208 |
| Climatology smoothing |100.0 | Pseudo-count shrinkage local→month/hour→global |
| PR-AUC tie tolerance |0.001 | Shortlist candidates cách best không quá tolerance |
| Brier tie tolerance |0.0001 | Sau shortlist PR, giữ gần minimum Brier |

Sampling dùng `pmod(xxhash64(station_id,epoch,seed),1_000_000)`; pilot<5,000,
final<20,000. Pilot là subset của final với cùng seed, trước threshold expansion; không lấy mẫu
copies độc lập. Selection/calibration/test hiện dùng tất cả eligible keys, natural prevalence.

Theo nhật ký feature run 11/10/2026, số eligible base keys là train 32,798,917,
selection 3,325,499, calibration 3,393,373 và test 6,374,476. Đây là số lịch sử cần đối chiếu
khi retry; không phải prevalence. Với config hiện tại, quy mô **ước tính** là pilot khoảng 164 nghìn
base keys/984 nghìn expanded rows, final train khoảng 656 nghìn/3.94 triệu expanded rows.
Exact counts phụ thuộc hash sample và phải lấy từ output; full selection có khoảng 19.95 triệu
expanded rows nên việc chấm 8 candidates tốn đáng kể, không suy runtime chỉ từ training fraction.

- [ ] Kiểm tra pilot/final train có đủ hai classes từng threshold trước fit; train support toàn bộ không bảo đảm pilot nhỏ có đủ positives cho rare thresholds.
- [ ] Distinguish training_stations lookup lấy toàn train với StringIndexer vocabulary fit trên 2%sample: station có trong train nhưng vắng trong sample vẫn có thể đi vào keep bucket. Thêm audit encoding-support, không đồng nhất lookup flag với đã thấy lúc fit.

**Pipeline thực tế:** StringIndexer(handleInvalid=keep) → OneHotEncoder(dropLast=false,
handleInvalid=keep) → median Imputer trên numeric columns → VectorAssembler.
LR thêm StandardScaler(withMean=false,withStd=true); GBT dùng assembled features không scale.
GBT hiện **one-hot station**, nên maxBins 64 không phải số station categories của một indexed categorical feature.

**Baseline:** histogram R30 trên toàn bộ eligible train_base; global empirical P(R30>tau),
broad `(positives+100×global_p)/(n+100)`, local `(positives+100×broad_p)/(n+100)`.
Unknown/sparse station fallback qua broad/global. LR/GBT chỉ fit 2% train, baseline dùng 100% train:
phải ghi training scope khác nhau; đây không phải equal-training-data comparison của cả ba models.

- [ ] Save pilot records cho 8 candidates và final records cho LR-A/LR-B/GBT-A/GBT-B, mỗi record gồm params/fit_seconds/support/selection metrics/path.
- [ ] Chọn best params riêng từng algorithm bằng selection B, rồi fit matched A/B trên cùng 2% sample. Chọn deployment giữa baseline/LR-B/GBT-B, ưu tiên baseline→LR→GBT khi PR/Brier tied; same algorithm tie dùng candidate ID hiện có, không tự gọi là ít cây hơn.
- [ ] Export `extractParamMap()` từ model đã fit để ghi cả defaults như LR tolerance/intercept và GBT stepSize/subsampling; không suy đoán defaults từ một Spark version khác.
- [ ] Freeze cả training scope, primary thresholds, calibration policy, fixed 0.5 reference cutoff và input-manifest hash; store immutable record trước test. Không optimize classification cutoff. Tăng fraction/tuning grid sau khi xem test là thay đổi protocol.

## 9. Metrics, calibration và final evaluation

Chi tiết: TASK_SPEC mục 12. **Chưa có final-test metrics hoàn chỉnh.**

- [ ] Khóa selection objective mean PR-AUC primary thresholds đủ classes từ train support; tolerance/tie-break Brier rồi model đơn giản, undefined fallback rule trước test.
- [ ] Đánh giá calibration trên selection; nếu ECE vượt trigger thì fit calibrator trên holdout Jul–Dec 2023. Lưu mapping/domain/fallback; không chọn cutoff hoặc bins trên test.
- [ ] Sau frozen manifest, notebook05 kiểm tra config/model root và từ chối overwrite final-evaluation; đánh giá climatology/LR-B/GBT-B trên cùng eligible 2024 cohort.
- [ ] Per-threshold metrics: base/expanded count, positives/negatives/prevalence, PR-AUC/ROC-AUC/Brier/log loss; precision/recall/F1 at fixed 0.5 are reference metrics only.
- [ ] Ghi Spark evaluator version/numBins; không gọi areaUnderPR là Average Precision; logloss epsilon frozen, AUC N/A nếu single-class, Brier skill chỉ khi denominator>0.
- [ ] Frozen macro weights; pooled-threshold metrics chỉ bổ sung. PR curves primary thresholds có prevalence reference/support, không giả định constant predictor area luôn bằng prevalence.
- [ ] Reliability diagram raw/calibrated: mean p/observed frequency/count mỗi frozen bin, cùng cohort.
- [ ] Monotonicity cùng station/time/history: threshold tăng thì probability không tăng; violation rate/max excess/tolerance raw và calibrated.
- [ ] Đánh giá unseen thresholds/domain và error strata station/month/threshold/unseen/onset/continuation định nghĩa bằng available history.
- [ ] Paired day/week block-bootstrap uncertainty: tất cả stations/thresholds cùng block, block length chọn train/validation, không bootstrap threshold copies độc lập. Ưu tiên Brier difference nếu hạn chế runtime, ghi metrics chỉ có point estimate.
- [ ] Lưu evaluation tables/curves/support/exclusions/run manifest; kết luận đúng kể cả ML không hơn baseline hoặc calibration không cải thiện.

### 9.1. Metric definitions và calibration policy

| Tham số/metric | Giá trị/công thức hiện có | Điều cần ghi rõ |
|---|---|---|
| `auc_num_bins` |1000 | Spark evaluator approximation; plot curve cũng có score-bin aggregation riêng |
| Brier |mean((p−y)^2) | Càng thấp càng tốt; same rows cho mọi model |
| Log loss |−mean(y log p +(1−y)log(1−p)) | Clip p bằng epsilon1e-12, không clip cho mọi metric khác |
| Reliability bins |10 equal-width | Bin=min(floor(10p),9), p=1 vào bin9 |
| ECE trigger |0.01 | Weighted absolute reliability gap trên selection, pooled threshold rows |
| Probability cutoff |0.5 | Fixed reference for precision/recall/F1 only; never optimized |
| Monotonic tolerance |1e-8 | Adjacent thresholds: positive excess>tolerance là violation |
| Bootstrap |1000 replicates, week blocks, seed 5208 |95% percentile CI của paired Brier differences |

Code hiện chọn calibrator **chỉ cho model deployment được selection chọn** khi selection ECE>0.01;
fit một pooled IsotonicRegression(p→label, isotonic=true) trên calibration holdout. Các model
còn lại có raw reliability assessment, không mặc định mỗi model đều có calibrated version.
Pooled calibration không bảo đảm calibration đúng ở từng threshold/station/month.

Không optimize classification cutoff. PR-AUC, Brier và reliability/calibration là kết quả
chính; precision/recall/F1 tại cutoff cố định 0.5 chỉ để tham khảo trên raw và calibrated
outputs. Isotonic calibration có thể đổi thang xác suất, nên 0.5 không được mô tả là cutoff
tối ưu hoặc là cùng operating point trước và sau calibration.

- [ ] Report raw comparison baseline/LR-B/GBT-B và selected_calibrated nếu có; cho biết deployment dùng version nào theo frozen rule, không chọn bằng test.
- [ ] PR primary thresholds chọn train support; macro equal weighting trên fixed primary list, ghi nhóm N/A rõ. Nếu nhóm thiếu class trên selection, ghi candidates được average trên những nhóm nào.
- [ ] Reliability output có threshold/bin/mean_p/observed_frequency/n; bins rỗng không tạoobserved frequency 0, có thể hiển thị trống.
- [ ] Weekly uncertainty join exact `(station,epoch,threshold)` keys và labels; all stations/thresholds cùngweek block, resample số weeks với replacement, weighted difference=sumlossdiff/sumrows.
- [ ] CI95 lấy empirical2.5/97.5 percentiles; code collect tối đa60 weekly blocks, ít hơn 2 blocks trả N/A. Chỉ đưa aggregates về Python, không collect predictions.
- [ ] Difference `Brier_B−Brier_A`: âm nghĩa B tốt hơn A; ghi pair/models/sign convention. Đây là pooled threshold-row difference, không tự gọi per-threshold CI.
- [ ] Monotonicity report adjacent-pair denominator, max excess và raw/calibrated; một pooled monotone mapping không sửa mọi vi phạm survival ordering của base model.

**Hoàn tất:** results có counts/exclusions/prevalence và probability-quality evidence, không chỉ AUC.
Đánh giá unseen midpoint thresholds là hỗ trợ trong domain, chưa chứng minh mọi ngưỡng thực đều tốt.

## 10. Báo cáo — ưu tiên Task 1 và Task 2

Chi tiết: TASK_SPEC mục 13.1–13.6; sửa tại `report/main.tex`, giữ editor hiện tại.

- [ ] Áp dụng `data/academic-writing-SKILL.md` khi viết; tiếng Anh rõ ràng, A4 một cột/font serif/captions/cross-references; Project1 layout tái dựng từ PDF do chưa có .tex/.cls/.sty.
- [ ] Bìa cá nhân đúng tên/ID; 10–12 trang thân bài chỉ là gợi ý, không phải page limit/rubric chính thức.
- [ ] Introduction ngắn: data2017–2024, hai tasks và phạm vi, không thêm research question/novelty.
- [ ] Dataset/cleaning: source/schema/mm/SGT, interval-end assumption/update semantics, bảng yearly raw/retained/stations/valid days/coverage/exclusions; giải thích missing≠0/conflicts/gaps/revisions và sample attrition.
- [ ] Computing environment: actual image/versions/master/workers/vCPU/RAM/disks/executors/config, pipelineflow/timings và Spark worker evidence; không dùng planned config như thực đo.
- [ ] Task1 khoảng2.5–3 trang: coverage → distribution → monthly/hourly → across-year/cohort sensitivity; số liệu thật, denominator/support, mỗi đoạn quan sát → cách tính → diễn giải → giới hạn.
- [ ] Task1 spatial/tail/complete-day/wet-spell đưa appendix khi không bổ sung finding chính; không gọi mạng trạm changing-cohort là climate trend.
- [ ] Task2 khoảng3–4 trang: công thức/timeline6 future intervals/strict>, input-output/domain/missing/unseen và retrospective limitation.
- [ ] Bảng features thực dùng có source/window/availability/missing rule; split table có base/expanded/positives/prevalence/purge.
- [ ] Models: baseline/LR/GBT, actual selected hyperparameters, train/tuning/selection/calibration; phân biệt rainfall threshold với probability cutoff.
- [ ] Main results: ba models cùng2024cohort, PR/ROC/Brier và per-threshold support; logloss/cutoff metrics bổ sung, macro/pooled/N/A rõ.
- [ ] Probability quality: reliability counts/raw-calibrated, PR curves/prevalence, A/B ablation, errors/unseen/monotonicity và uncertainty phù hợp evidence.
- [ ] Discussion: kết nối patterns với features, rainfall-only/missing/revisions/rare heavy rainfall/record length/domain/unseen/availability limits; không overclaim real-time/robust/significant.
- [ ] Reproducibility: README/exact runtime/config/commit/manifest/model package/loadpredict; conclusion trả lời cả tasks, không thêm kết quả mới.
- [ ] Viết abstract150–200 từ cuối cùng với ít nhất một finding thật mỗi task; cập nhật conclusion khi final results có, không giữ metric unavailable như bản nộp hoàn chỉnh.
- [ ] References nguồn thật, lecture title/page khi dùng; AI declaration đúng công cụ/model/công việc hỗ trợ và trách nhiệm cá nhân.
- [ ] Figure/table index nối source output/run/split/cohort/config/commit; tạo numeric macros từ frozen outputs, số abstract/bảng/conclusion khớp rounding.
- [ ] Caption tự hiểu được, axes units/legend/A4 readability, dẫn hình trong text; dùng `quality_by_year.csv`, coverage/monthly/hourly/annual figures, model/threshold tables, PR/calibration và ablation/errors khi có evidence.
- [ ] Dùng built-in LaTeX compiler cho file hiện tại, sửa lỗi/references/overflow; render và đọc toàn bộ pages A4, không TODO/placeholder/brokenref trong PDF nộp.
- [ ] Requirement-to-section/page checklist đủ cleaning, within/acrossyear, features/models/environment/performance/limitations/AI; đối chiếu từng số với artifact.

### 10.1. Thiết kế nội dung báo cáo và bằng chứng cho từng phần

Báo cáo là trọng tâm bàn giao. Viết methods từ config/artifacts đã xác nhận trước,
viết findings sau khi có outputs; không kéo dài lý thuyết để bù thiếu kết quả.
Mỗi claim định lượng phải nối được tới CSV/JSON/run và caption có cohort/mẫu số.

| Section | Nội dung cụ thể | Evidence cần có trước khi viết kết luận |
|---|---|---|
| Dataset/preprocessing |8 years,12 fields, timezone/interval assumption, cleaning rules | Inventory + yearly quality + disjoint exclusions/coverage |
| Computing environment | Actual distributed config và thời gian từngphase | Clusterdescribe, SparkConf/executors, actionindex/timings |
| Task1 distribution | Dry mass, wet frequency/intensity/tail | distribution_by_year/histogram, valid/wet counts |
| Task1 within-year | Monthly/hourly extrema và consistency | eligible group stats, denominator/support, figures |
| Task1 across-year | Cohort availability/sensitivity, descriptive changes | annual cohort support + common/descriptive tables |
| Task2 design | R30 strict >, windows, features/splits/availability | feature dictionary, config, sample attrition/purge |
| Task2 training | Baseline100% vs ML2%, pilot0.5%, parameters | candidate records/finalists/frozen manifest |
| Task2 performance | Same2024cohort, per-threshold/probabilityquality | raw/calibrated metrics/reliability/PR/support/errors |
| Discussion/conclusion | Findings trả lời Tasks1/2 và giới hạn áp dụng | Main text figures/tables, không thêm kết quả mới |

**Thiết kế đoạn kết quả:** câu1 nêu observation có value/unit; câu2 giải thích statistic/cohort;
câu3 diễn giải trong phạm vi dữ liệu; câu4 nêu limitation nếu có ảnh hưởng. Ví dụ month có mean
observed rainfall cao nhất không đồng nghĩa tháng có full-month rainfall cao nhất nếu coverage khác.
Khi common cohort rỗng, viết kết quả đó như hạn chế của annual comparison, không để biểu đồ trống không giải thích.

- [ ] Data-quality table chứa đủ8 years và retained/rejected/coverage; environment table tách requested/actual khi khác; split table có eligibility/base/expanded/sample fraction.
- [ ] Model comparison table ghi baseline/LR-B/GBT-B raw và selected_calibrated riêng, training scope, PR/ROC/Brier/logloss; per-threshold table thêm positives/prevalence/N/A.
- [ ] PR/reliability figures giới hạn số lines, dùng primary thresholds frozen; calibration bin counts trong subplot/bảng kèm, không che bins thiếu support.
- [ ] A/B table trên selection cho cùngkeys/grid/hyperparameters; error summary chọn month/station/onset/unseen có support rõ, appendix giữ bảng đầy đủ.
- [ ] Figure/table provenance record gồm artifact URI/localpath, run/attempt, config hash, analysedyears/split, cohort/weighting/missingpolicy và formatter/rounding.
- [ ] Macros cho số lặp ở abstract/table/conclusion; bảng ghi precision hợp lý (coverage/percent, rainfall mm, metric decimals), tránh nhiều chữ số tạo độ chính xác giả.
- [ ] Appendix có screenshotindex và ảnh thực, full config/feature dictionary/threshold tables, AI usage. Mỗi appendix được dẫn từ thân bài nếu cần người đọc kiểm chứng.
- [ ] Sau compile đọc từngpage: float position, table width, axes/fonts/legend, references, captions và equations; PDFdraft compile được chưa chứng minh ready-to-submit.

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

### 11.1. Thiết kế deployment package và inference contract

Package đề xuất tên `DSA5208_Project2_A0355091E/`; layout README, requirements, config mẫu,
notebooks, src/scripts, `models/deployment/`, fixtures và report.pdf. Đây là cấu trúc bàn giao,
không phải xác nhận archive hiện đã có. Giữ source LaTeX/figures/tables để reproduce PDF khi phù hợp upload limit.

| Thành phần model | Nội dung phải lưu | Cách kiểm tra |
|---|---|---|
| `selected/` | PipelineModel hoặc baseline histogram tree | Load đúng algorithm, đủ mọi Spark part/metadata files |
| `calibrator/` | Isotonic model khi use_calibration=true | Mapping dùng sau raw scoring giống evaluation |
| `training_stations/` | Trainstationlookup | Unseen flag giống run |
| `metadata.json` | Algorithm/config/use_calibration/selected/input hash | Bổ sung runtime/commit/domain/tolerance nếu code hiện chưa lưu đủ |
| History fixtures | Tiny station/epoch/rainmm/rainunits data | Không cần truy cập private bucket để chạy example |
| Expected predictions | Query/probability/status/mode | Compare loaded output trong frozen tolerance |

`predict(station_id,prediction_time,threshold_mm)` cần ISO8601 có timezone, exact5-minute grid,
finite threshold trong[0.2,10] với độ chính xác scale hỗ trợ. History query lấy180 phút
cùngstation ở/beforet. Không có reading trong cửa sổ trả insufficient_history; không có reading
hiện tại trả `missing_current_observation` với probability null. Unseen station có current
reading vẫn nhận probability cùng status cảnh báo, không chứng minh khả năng generalize.

- [ ] Status contract hiện có: `ok`, `invalid_station`, `invalid_time_or_threshold`, `off_grid_time`, `threshold_out_of_domain`, `insufficient_history`, `missing_current_observation`, `unseen_station`; missing-current returns no probability, while valid/unseen predictions include current_missing=false.
- [ ] Xác định xử lý threshold nhiều hơn6 decimals: feature conversion hiện có thể raise; cần quyết định status/error contract và document nhất quán, không silently round query.
- [ ] Synthetic dryhistory36 readings giúp chạy inference; bổ sung valid wet/gaps/unseen/domain fixtures và assert missing-current returns no probability.
- [ ] Chốt reload comparison tolerance đề xuất absolute 1e-8 trước xuất expected fixtures; xác nhận actual reproducibility trên same runtime rồi lưu giá trị này trong package.
- [ ] Notebook05 hiện reload trong cùng Spark session; cần session/kernel mới cho acceptance fresh-session, không gọi current `RainfallPredictor.load()` là đã hoàn tất acceptance.
- [ ] Run hướng dẫn từ archive đã giải nén và portable input path; README ghi rõ inference vẫn cần Spark/runtime tương thích, chưa phải webservice hoặc real-time deployment.

## 12. Spark UI evidence cho mọi lần chạy

Chi tiết: TASK_SPEC mục5/13.5. Evidence root `report/evidence/spark-ui/<RUN_ID>/index.csv`.

- [x] Có index/API metadata và ảnh `20261011-full-01/app5-jobs-summary-active-checkpoint.jpg` kèm note. Đây là active checkpoint lưu từ trang cũ, **không chứng minh terminal status**.
- [ ] Chạy notebook11 cập nhật index một dòng/action: command/notebook cell, RUN_ID/Application ID/Job IDs/final status/duration/stages/tasks, summary path và mọi detail paths.
- [ ] Sau action kết thúc, mở đúng Spark application, tab Jobs; nếu jobs còn chạy thì chờ terminal state và chụp lại.
- [ ] Chụp summary và mọi Job detail từ **Spark UI thật**, lưu tên có Application ID/Job ID/status. Không thay bằng Dataproc Jobs/YARN/API JSON hoặc ảnh minh họa.
- [ ] Capture cả failed/cancelled actions; nếu không có Job ID giữ summary và ghi nojobs. Nếu không khôi phục được event logs/UI thì ghi thiếu evidence, không tự điền trạng thái.
- [ ] Bổ sung app1/app3/app4/app5 detail/summary còn thiếu bằng History Server nếu logs khôi phục được; checkpoint app5 không thay thế các ảnh này.
- [ ] Đối chiếu index với notebook/log/UI, mọi Job ID có ảnh; chèn ảnh/index vào report appendix để xem lại.
- [ ] Download evidence trước teardown; ghi missing screenshots trung thực nếu chưa hoàn thành.

### 12.1. Thiết kế evidence index và cách chụp

Một action có thể tạo hàng trămjobs; không gộp tất cả thành một ảnh rồi bỏ mất jobdetails.
Dùng action/jobgroup để map notebook cell với Job IDs, Application ID duy nhất; index chính một dòng/action,
có danh sách jobs/details hoặc trỏ job manifest con. API hỗ trợ inventory nhưng screenshots phải từ trang UI.

| Trường index | Dữ liệu cần ghi |
|---|---|
| run_id/action_id | Run và tên action/notebook cell, code/command description |
| application_id | App đúng Spark session, không lấy Dataproc job ID làm Spark Job ID |
| start/end/status | UTC timestamps và terminal state; incomplete nếu chưa biết |
| job_ids/job_statuses | Toàn bộ job IDs của action, trạng thái từng job |
| duration/stages/tasks | Theo UI, kể cả failed/retried attempts nếu hiển thị |
| summary_screenshot | File ảnh Jobs summary sau terminal state |
| job_detail_screenshots | File cho từng Job ID hoặc manifest map ID→path |
| evidence_note | Missing logs, cached checkpoint, nojobs, viewport limitation |

- [ ] Filename đề xuất `<APP_ID>__<ACTION_ID>__summary__<STATUS>.png` và `<APP_ID>__job-<ID>__<STATUS>.png`; ảnh gốc không cắt mất identity/status/stages/tasks.
- [ ] Nếu page dài, chụp thêm viewport/scroll ảnh nối tiếp có suffix part; vẫn giữ ảnh identity để xác định đúng job, không chỉnh số/state trong ảnh.
- [ ] Chụp sau cell/action nhưng trước application stop khi thuận tiện; nếu jobs async còn active, lưu checkpoint riêng rồi capture terminal state sau.
- [ ] History Server restore dùng original event logs; giữ đúng retry/app distinctions, không lấy job ID cùng số từ application khác.
- [ ] Có nojobs thì summary + note; có thiếu detail thì đánh dấu pending/missing thật. Report appendix có representative images và link/index đầy đủ để truy lại mọi job.
- [ ] Review coverage: set job IDs từaction logs bằng set job IDs códetail paths; mọi referencedfile tồn tại, status/duration khớpUI và screenshot note.

Checkpoint app5 cũ vẫn giữ giá trị mô tả quá trình nhưng chưa phải terminal screenshot.
Thu ảnh tốn thời gian với1879+jobs; yêu cầu của người dùng vẫn giữ, nên thu theo từngaction ngay trong session
và lưu event logs bền vững. Không giảm yêu cầu thành chụp một vài representativejobs để tick acceptance.

## 13. Shutdown và kiểm tra hoàn thành

- [ ] Export notebooks, persist/download cleaned/features/models/results/manifests/evidence; xác minh local bản bàn giao trước xóa cloud artifacts cần dùng.
- [ ] Xóa đúng cluster qua notebook00, lưu output; truy vấn lại project/region cluster listing.
- [ ] Kiểm tra VM/disks/reservedaddresses/tài nguyên phụ; không suy ra đã cleanup chỉ từ notebook mất kết nối.
- [ ] Dọn GCS staging/temp/raw/outputs không còn cần sau download/check, giữ retention rõ; cluster deletion không xóa storage charges.
- [ ] Nếu đối soát chi phí sau run thì ghi snapshot/account-level/project-level/pending trung thực; không dùng dữ liệu chưa xác minh làm actual runtime cost trong report.
- [ ] Audit cuối: cleaning khép kín; Task1 đủ 8 năm; target/leakage/splits đúng; ba models/calibration/A–B/monotonicity/unseen/metrics có evidence; freeze trước test; ảnh từng job; notebook outputs; fresh-session reload; PDF/package/receipt; cleanup.

### 13.1. Teardown design và completion audit

Teardown là bước của workflow, không phải thao tác tự động sau một cell lỗi khi artifacts chưa export.
Không xóa raw/model/results cloud trước khi bản local đã download và được đối chiếu.

- [ ] Trước stop/delete: liệt kê expected artifacts theo run/attempt, kiểm tra notebooks cóoutput, model tree đầy đủ, metric/figure/index files tồn tại; download records/config/source hash cùng model.
- [ ] Dùng manifest files/bytes/hash cho localarchive, đối chiếu archive sau giải nén; Spark model directories phải có các part files và metadata, không chỉ thư mục rỗng.
- [ ] Sau delete đúngcluster, querylisting trong cùngproject/region và kiểm tra VMs/disks/reservedaddresses bằng identities đã ghi; report kết quả thực, nếu delete pending chưa gọi deleted.
- [ ] Giữ bucket cho export cuối nếu cần; lập danh sách prefix staging/temp/output redundant để dọn sau khi xác nhận, không xóa theo wildcard rộng có thể chứa runs khác.
- [ ] Lưu cleanup notebook output/resource inventory cùngrun. Nếu còncloud resource cần giữ thì ghi purpose/retention, không claim toàn bộ tài nguyên đã ngừng.

**Audit bàn giao** dùng mapping requirement→section/page→artifact→run/config, không chỉ checklist tick.
Mỗi requirement phải có evidence: withinyear/acrossyear, cleaning/quality, probabilitytarget,
features/split/models, actualenvironment/performance, limitations/AI và trainedmodelload.
Việc nộp trênCanvas cần receipt riêng; model package/report sẵn sàng không đồng nghĩa đã nộp.

**Ưu tiên lịch thực hiện:** đóng cleaning và modelrun trước, rồi finaltest/fullEDA, tiếp đến viết report
và archive/reload; dành lượt cuối để đọcPDF/đối chiếuảnh và upload. Mốc18/10/2026 23:59 chưa có
Canvas timezone xác nhận thì giữ TODO tương ứng, không tự tính deadline từ timezone máy.

## 14. Extensions — chỉ sau phần cốt lõi

- [ ] Nếu cần và còn thời gian: temporal folds đúng order/purge, availability-delay sensitivity chọn bằng train/validation.
- [ ] Optional C=B+neighbours: time-valid locations/as-of filter, không chọn correlation2024; same budget/intersection comparison và coverage riêng.
- [ ] Optional monotonic projection/interpolation: freeze và lưu trong inference pipeline trước test, không chỉ sửa plot.
- [ ] Optional thêm rolling/spatial/scaling experiments chỉ khi giúp yêu cầu đề; không trì hoãn report/model package để mở rộng scope.

Không đánh dấu toàn dự án hoàn thành vì TODO đã được viết. Các ô chưa tick là công việc thực thi còn lại;
nhật ký đầy đủ và các lần chạy lỗi/dở dang được giữ nguyên trong TASK_SPEC.

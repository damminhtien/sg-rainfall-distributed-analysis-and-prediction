> Bản đặc tả và nhật ký lịch sử được giữ nguyên từ TASK.md khi chuyển sang TODO ngày 12/10/2026.
> Dùng [TASK.md](TASK.md) để xem việc cần làm và chỉ dẫn hiện hành. Các yêu cầu chờ Reports/Cost table,
> cost gate đóng và mức dự phòng $40/$10 dưới đây là chính sách cũ, đã được chỉ dẫn mới của người dùng
> “chạy tiếp, không cần quan tâm tới cost” thay thế về việc chờ đối soát chi phí.
> Ràng buộc chỉ dùng Education account và credit 3LU63BPGY77JFDQQ vẫn có hiệu lực.
> Các trạng thái cloud trong nhật ký là snapshot lịch sử, không phải kiểm tra live.

# DSA5208 Project 2 — Singapore Rainfall Analysis and Prediction

Kế hoạch đã review ngày 11/10/2026. Đây là kế hoạch thực hiện, chưa phải bằng chứng rằng data đã được kiểm định hoặc cluster/model đã chạy. Mỗi phase có output và acceptance criteria. Các lựa chọn statistical/modeling phải được ghi lại trước final test.

## 1. Yêu cầu chính thức và lựa chọn triển khai

Nguồn đề bài: `data/project_description.txt`. Nguồn data: [Historical Rainfall across Singapore, NEA](https://data.gov.sg/collections/2279/view).

| Nội dung | Yêu cầu chính thức |
|---|---|
| Data | Các năm 2017–2024; quan trắc mưa mỗi 5 phút tại các trạm Singapore |
| Task 1 | Phân tích kiểu mưa trong năm và thay đổi giữa các năm, có hình và thảo luận |
| Task 2 | Xác suất một trạm tích lũy **hơn** ngưỡng mưa được yêu cầu trong 30 phút tiếp theo |
| Công nghệ | Structured data processing bằng Spark; xây dựng và đánh giá model bằng Spark MLlib |
| Report | Cleaning, patterns, features, model/results, computing environment/cluster configuration, limitations và AI usage |
| Bàn giao | Source + README; trained model; PDF ghi tên và student ID của tất cả thành viên |
| Nhóm/deadline | Tối đa 3 người; 18/10/2026, 23:59 theo mô tả Canvas; kiểm tra timezone/upload limit trên Canvas |

**Triển khai cá nhân (1 người):** Mac + gcloud → GCS → Dataproc + Jupyter + Spark/MLlib → GCS outputs → source/model/PDF. Không dựng cluster local. Đề cho phép local/GCP/Databricks; Google Cloud là lựa chọn triển khai, không phải yêu cầu bắt buộc của đề.

Các citation placeholder cũ (`Lec6`, `Project-2-Singapore-Rainfall-An…`) đã được bỏ. Khi dùng lecture trong report, đọc source và ghi tên slide/số trang chính xác.

## 2. Kết quả review

| Điểm yếu của bản cũ | Cách làm rõ/sửa |
|---|---|
| Upload toàn bộ `data/raw/`, giả định layout chưa kiểm tra | Dùng data ngoài repo; manifest chọn đúng tám file 2017–2024, loại 2016 |
| Giả định timestamp đúng lưới 5 phút | Xác minh interval semantics và alignment trước label |
| `observation_time ≤ t` được coi là đủ chống leakage | Kiểm tra cả availability/revisions; nêu giới hạn real-time |
| Year split có thể để label vượt sang năm tiếp theo | Purge horizon vượt split/fold và kiểm tra label endpoint |
| Duplicate/gap/coverage chưa có policy | Data contract, quarantine, mẫu số và attrition rõ |
| Heatmap được xem là chứng minh không có bias | Cohort trạm ổn định, coverage và sensitivity analysis |
| Training thresholds thay cho input ngưỡng tùy ý | Tách grid/domain/inference; kiểm tra ngưỡng chưa thấy |
| Tuning và calibration chưa tách dữ liệu | Temporal selection và calibration holdout riêng trong 2023 |
| PR-AUC chưa đủ cho xác suất | Thêm Brier/log loss/calibration, prevalence và uncertainty |
| Cloud thiếu IAM/network/runtime/cost guardrails | Preflight, worker identity, pinned image, max-age và persist artifacts |
| Model save chưa chứng minh reproduce được | Lưu toàn bộ pipeline/config; reload và infer từ submission package |

**Các gate còn cần evidence:** timestamp là đầu hay cuối interval; nghĩa của update fields; quy tắc timestamp alignment; billing/project/IAM; image/runtime tương thích; source LaTeX Project 1. Skill academic-writing đã có tại `data/academic-writing-SKILL.md` và đã đọc khi lập khung báo cáo. Không tự coi các gate này đã đạt.

## 3. Phase 0 — Repository và provenance

Đường dẫn local lấy từ `data/pwd`:

- Dataset root `/Users/macbook/Data/sg historical rainfall`, CSV ở `raw/`.
- Course `/Users/macbook/Desktop/NUS/DSA5208`, Project 1 ở `project1/`.

Full scan cục bộ 11/10/2026: thư mục `raw/` hiện có đúng tám CSV cần dùng, 2017–2024; chưa thấy file 2016 tại đường dẫn này. Ghi chú inventory cũ từng ghi chín file gồm 2016 nên không còn đúng với inventory hiện tại. NEA collection metadata công bố chín annual datasets từ 2016 đến 2024; assignment chỉ yêu cầu 2017–2024 ([collection](https://api-production.data.gov.sg/v2/public/api/collections/2279/metadata)).

`scripts/build_manifest.py` đã đọc đủ 8,047,044,701 bytes / 48,538,713 data rows, tính SHA-256 mỗi file, xác nhận các file có cùng 12-column header, không có row sai field-count và không có timestamp thiếu/parse lỗi. Kết quả theo file, dataset IDs và timestamp min/max nằm trong `manifests/input_manifest.json`. Tất cả record timestamps dùng `+08:00` trong dữ liệu đã quét. Các khoảng quan sát lần lượt bắt đầu 2017 `08:04:59`, các năm 2018–2024 lúc `00:00:00` và kết thúc `23:55:00` mỗi năm. Đây mới là inventory/schema/timestamp parse scan; **chưa** kiểm tra value range, duplicates/conflicts, station coverage, gaps, timestamp alignment theo trạm hay interval semantics. Không dùng min/max làm bằng chứng dataset đầy đủ.

Header/sample ban đầu cho thấy update delay (2017 observation `08:04:59`, update `08:13:31`; 2024 observation `00:00:00`, update `00:05:26`). Đây là lý do tiếp tục audit availability/alignment, không chứng minh `timestamp` là interval end.

Giữ raw ngoài repo và bất biến. Chỉ chọn tám file `HistoricalRainfallacrossSingapore{2017..2024}.csv`. Không dùng 2016 ngầm làm warm-up. Code nhận `DATA_ROOT` từ config; không hard-code đường dẫn cá nhân. Manifest phải có dataset ID/source URL, năm, filename, bytes, SHA-256, retrieval time, schema và actual time range. Đối chiếu manifest local/GCS; object listing chưa chứng minh upload đúng.

```text
sg-rainfall-distributed-analysis-and-prediction/
├── README.md
├── requirements-local.txt
├── requirements-cluster.txt
├── config/
│   ├── cloud.env.example
│   └── experiment.json
├── data/TASK.md
├── notebooks/
│   ├── 00_environment.ipynb
│   ├── 01_data_quality.ipynb
│   ├── 02_task1_weather_patterns.ipynb
│   ├── 03_task2_features.ipynb
│   ├── 04_task2_models.ipynb
│   └── 05_final_evaluation.ipynb
├── src/rainfall/             # config, cleaning, features, models, metrics, predict, plotting
├── scripts/
├── tests/
├── figures/
├── results/
├── models/
└── report/                  # main.tex, references.bib
```

**Jupyter Notebook là entry point duy nhất để chạy code của project và xem output.** Các hàm tái sử dụng nằm trong `scripts/` hoặc `src/`, được import và gọi từ notebook; không chạy phân tích bằng terminal, `spark-submit` hay batch script. Lệnh Cloud SDK cần thiết cũng được gọi trong cell Python bằng `subprocess`, có in trạng thái/kết quả vào output. Notebook được lưu cùng output cần thiết; credential và nội dung nhạy cảm không được ghi vào notebook. Mỗi run có `RUN_ID`, Git commit, input manifest hash và config hash; ghi output dưới run prefix riêng, không overwrite final results. Không commit `skills/`, `.codex/`, `.vscode/`, `AGENTS.md`, `config/cloud.env`, credential hoặc raw/cache lớn. Plan/config mẫu và bảng kết quả nhỏ có thể commit.

**Acceptance:** manifest đủ tám năm; portable paths; entry point và execution order rõ; repo không chứa credential/raw.

## 4. Phase 1 — Cloud preflight và budget

### 4.1. Credit và billing duy nhất được phép sử dụng

**Ràng buộc của người dùng: chỉ dùng khoản education credit dưới đây và đúng billing account chứa khoản credit đó. Tuyệt đối không dùng billing account khác, credit khác hoặc chi tiền ngoài khoản credit này.** Ràng buộc áp dụng cho toàn bộ project: compute, Dataproc, GCS, disk, network, logging và các tài nguyên phụ.

Credit ID `3LU63BPGY77JFDQQ` là mã duy nhất người dùng cho phép; người dùng chỉ định dùng Billing Account có tên Education. Bảng giữ snapshot trước cloud run ngày 11/10/2026 và ghi riêng snapshot Credits mới nhất nhìn thấy ngày 12/10/2026. Số dư mới hơn vẫn chưa đủ để duyệt job vì Reports, Cost table và pending usage chưa được đối soát:

| Thuộc tính | Giá trị được cung cấp |
|---|---|
| Credit name | Scalable distributed computing for data science aug26 |
| Credit ID | `3LU63BPGY77JFDQQ` |
| Status / remaining at pre-run snapshot | Available / 100% (historical only) |
| Remaining / original value at pre-run snapshot | $50.00 / $50.00 (historical only) |
| Latest Credits page shown 12/10/2026 | Available / 96%; $47.91 remaining / $50.00 original |
| Type | One-time |
| Usage scope | Any service on this billing account |
| Start / end date | 17/08/2026 / 14/08/2027 |
| Credit application type | Net pricing |

Credit ID không phải Billing Account ID. Read-only gcloud inventory ngày 11/10/2026 xác định account được người dùng chỉ định là **Billing Account for Education**, ID `01DAD8-AE5F1A-E3910A`, trạng thái open, currency USD. Không dùng account khác. Gcloud xác nhận tên/ID/trạng thái account, không chọn một Credit ID cho từng job. Snapshot Console trước run ngày 11/10/2026 hiển thị credit `3LU63BPGY77JFDQQ` Available, 100%, USD 50.00 remaining; tab đó đã mở trước run nên là số liệu lịch sử. Credits page đang mở trong Chrome ngày 12/10/2026 hiện hiển thị đúng Education account và 96%, USD 47.91 còn lại trên USD 50.00. Chênh lệch USD 2.09 là mức giảm của credit cấp account; chưa quy cho riêng project này nếu chưa xem cost breakdown. Reports, Cost table và pending usage chưa được xác minh trong lần kiểm tra mới.

Google áp dụng promotional credits tự động vào eligible costs; gcloud không phải cơ chế chọn một Credit ID cho từng job. Kiểm tra Credits và cost details của đúng account để xác minh khoản nào được áp dụng ([education credits](https://docs.cloud.google.com/billing/docs/how-to/edu-grants), [cost details](https://docs.cloud.google.com/billing/docs/how-to/cost-table)). Nếu có nhiều promotional credits cùng scope và chưa xác minh được chỉ khoản chỉ định sẽ chi trả, dừng bước có phí để làm rõ. Không redeem thêm credit, đổi account, thêm phương thức thanh toán hoặc chuyển sang trả tiền để tiếp tục.

Project chạy được là `sg-rainfall-dsa5208-261011`; ngày 11/10/2026 notebook preflight đã xác minh `billingEnabled=true` và đúng billing account `01DAD8-AE5F1A-E3910A` trước khi tạo tài nguyên. Credits page hiện hiển thị USD 47.91 còn lại, nhưng Reports, Cost table và pending usage chưa đối soát; do đó chưa xác định được khoản giảm USD 2.09, gross usage của project, khoản pending cấp account hoặc storage cost hiện tại. Billing gate vẫn đóng và không tạo thêm tài nguyên có phí. Việc áp dụng credit diễn ra tự động ở cấp billing account, không thể gán Credit ID riêng cho một job. Trước lần chạy cloud tiếp theo, phải xem cost details của đúng account và bảo đảm giới hạn USD 40 chi tiêu cùng USD 10 dự phòng vẫn còn nguyên sau khi tính usage/pending. Không đổi billing account, không redeem credit khác, không thêm payment method hoặc trả tiền ngoài credit được chỉ định. Dọn cluster và mọi tài nguyên có phí sau khi xuất kết quả; đối soát chi phí cuối có thể chậm.

### 4.2. Cấu hình và preflight

Thông số project/account/region/run được đặt trong `notebooks/00_environment.ipynb`; không dùng file credential hay `config/cloud.env` làm đường chạy riêng. Preflight đã xác minh đúng Education billing link trước khi tạo bucket/cluster; project, bucket, VM service account, region và quota được ghi trong outputs. Quyền đọc trạng thái billing của dedicated VM service account ban đầu thiếu, nên đã thêm project-scoped custom role chứa duy nhất `resourcemanager.projects.get`, sau đó xác minh lại billing account chính xác. Cluster riêng tư Dataproc/YARN thuộc lượt chạy ban đầu đã bị xóa theo inventory read-only mới nhất; không có cluster hiện hành. Notebook 01 đã đối chiếu SHA-256 byte thực tế của tám CSV với manifest. Đối soát usage và credit sau cleanup vẫn là bước bắt buộc.

Preflight trong notebook: kiểm tra CPU/disk quota theo region, subnet/Private Google Access/firewall cho trao đổi nội bộ, quyền tạo cluster và `iam.serviceAccounts.actAs`. VM service account cần `roles/dataproc.worker` cùng quyền đọc/ghi data và staging/temp buckets phù hợp; không đưa JSON keys vào notebook. Chỉ đánh dấu gate đạt khi đã có output kiểm tra. Nguồn: [Dataproc service accounts](https://docs.cloud.google.com/dataproc/docs/concepts/configuring-clusters/service-accounts).

### 4.3. Giới hạn vận hành trong credit

Mục tiêu chi tiêu dự toán tối đa **$40**, giữ ít nhất **$10** dự phòng cho pending usage, storage, export/download và cleanup, cùng currency với credit. Đây là ngưỡng vận hành bảo thủ, không phải spending cap do Google bảo đảm. Khi remaining credit ≤ $10 hoặc dự toán session + pending/retention costs vượt phần cho phép, không bắt đầu job/session mới; persist/download outputs và ngừng tài nguyên có phí của project.

Tạo project-scoped alerts-only budget trên đúng account, theo dõi usage costs và credit application riêng; không để net cost sau credit bằng 0 che lượng credit đã tiêu. **Alerts-only budget không tự chặn chi tiêu** ([Billing budgets](https://docs.cloud.google.com/billing/docs/how-to/budgets)). Dự toán gồm VM, Dataproc fee, disk, GCS operations/storage/network và tài nguyên phụ; lấy giá theo region tại ngày chạy từ [pricing](https://cloud.google.com/dataproc/pricing). Ghi credit snapshot đầu/cuối session và usage chưa phản ánh trên Console; billing có thể cập nhật trễ ([cost details](https://docs.cloud.google.com/billing/docs/how-to/cost-table)).

Cluster có idle timeout **và** maximum lifetime, vẫn xóa thủ công khi xong. Xóa cluster không làm hết phí GCS/tài nguyên phụ. Không tạo cluster trước khi preflight và dự toán đạt.

**Acceptance:** `BILLING_ACCOUNT_ID` đã khóa với credit `3LU63BPGY77JFDQQ`; không account/credit khác; linkage và balance/scope được đối chiếu; budget đủ cả pending/storage/cleanup; quota/IAM/network đúng. Chưa đạt thì không thao tác có phí.

## 5. Phase 2 — GCS, Dataproc và Jupyter

Bucket private, uniform access, cùng region với compute. Tạo bucket và upload đúng manifest cùng 8 CSV (2017–2024) bằng các cell riêng trong `00_environment.ipynb`; mỗi cell in trạng thái và danh sách object vào output. Không upload 2016 hoặc dữ liệu ngoài manifest.

Layout: `raw/`, `manifests/`, `processed/<RUN_ID>/`, `features/<RUN_ID>/`, `notebooks/`, `models/<RUN_ID>/`, `results/<RUN_ID>/`, `staging/`, `temp/`.

Cloud runs đọc paths từ manifest, không wildcard lấy nhầm năm/object. GCS raw + manifest là nguồn input; không copy raw vào mỗi worker.

Ứng viên cluster ban đầu: 1 master `e2-standard-4` + 2 workers cùng loại (4 vCPU, 16 GB RAM/VM), disk 100 GB/VM, không GPU. Đây là cấu hình cần price check và smoke run; master 8 GB bản cũ ít dư địa cho Jupyter/driver. Không tăng compute trước khi kiểm tra Spark plan/skew/cache/partitions.

Chọn supported image, kiểm tra Jupyter/Python/Spark, pin **exact image version** sau smoke run. Khi thử bằng alias phải ghi resolved version. Local dùng `python3` 3.14 cho utilities phù hợp; cloud dùng Python đi kèm image tương thích Spark. Không ép Dataproc sang 3.14/cài PySpark mới đè runtime ([image versions](https://docs.cloud.google.com/dataproc/docs/concepts/versioning/dataproc-version-clusters)).

Tạo Dataproc cluster bằng cell riêng trong `00_environment.ipynb` sau khi price check và cost guard đạt. Cluster bật Jupyter component, dùng private networking, một master và hai workers theo cấu hình ứng viên; cell in tên cluster, region, image version, VM/disk và lifecycle settings để xem lại trong notebook. Không tạo cluster mặc định khi cost estimate hoặc credit gate chưa đạt.

`3h` là lifetime session ban đầu. Idle calculation dựa trên Dataproc/YARN activity; notebook application có thể giữ cluster active. Max-age vẫn xóa cluster khi job đang chạy, làm job thất bại. Lưu checkpoint/output theo phase và theo dõi lifecycle ([scheduled deletion](https://docs.cloud.google.com/dataproc/docs/concepts/configuring-clusters/scheduled-deletion)).

Internal-IP cluster cần Private Google Access và network nội bộ đúng. Public PyPI/Maven downloads có thể thất bại khi thiếu internet egress; ưu tiên dependencies có sẵn hoặc compatible wheels trên GCS, tính thêm chi phí nếu dùng NAT ([network configuration](https://docs.cloud.google.com/dataproc/docs/concepts/configuring-clusters/network)). Pin/import-check SciencePlots/Matplotlib theo Python cloud trước full run.

Mở Console → cluster → Web Interfaces → Jupyter/JupyterLab qua Component Gateway. Lưu notebook trên GCS và export về repo sau session; không chỉ giữ artifact trên local disk cluster ([Jupyter component](https://docs.cloud.google.com/dataproc/docs/concepts/components/jupyter)).

Trong notebook Dataproc, ghi image, Spark/Python/Java/OS, region, machine types/disks, service account, application ID/master, actual executors và Spark config. Đặt `spark.sql.session.timeZone=Asia/Singapore`. Notebook smoke run đọc một CSV với explicit schema, tạo/read Parquet nhỏ và kiểm tra MLlib/import/GCS. Spark master phải là YARN, executors chạy trên workers. Mọi stdout, bảng và hình cần cho analysis hiển thị trong notebook; lưu notebook đã chạy lên GCS và export về repo sau session.

**Acceptance:** worker identity đọc/ghi GCS; Spark distributed; notebook persist; environment/lifecycle được ghi.

**Bằng chứng Spark Web UI cho từng lần chạy:** mọi notebook action
đã tạo Spark application/job phải được theo dõi đến trạng thái cuối (thành công,
thất bại hoặc bị hủy). Sau khi cell/action kết thúc, mở đúng application bằng
Application ID: dùng Spark Web UI đang hoạt động nếu notebook/application còn mở,
hoặc **Spark History Server** qua Web Interfaces/Component Gateway nếu application
đã kết thúc. Kiểm tra tab Jobs. Chụp ảnh trang tổng quan Jobs và trang chi tiết
của từng Spark Job ID; ảnh phải cho thấy Application ID/Job ID, trạng thái,
thời lượng và thông tin
stages/tasks hiện có. Nếu job còn chạy sau khi lệnh trả về, chờ trạng thái cuối
rồi chụp lại. Không dùng trang Dataproc Jobs hoặc
YARN thay cho ảnh Spark UI. Bật/kiểm tra Spark event logging và History Server
trước batch runs để các ứng dụng đã hoàn tất còn xem được. Lưu ảnh vào
`report/evidence/spark-ui/<RUN_ID>/` với tên có Application ID, Job ID và trạng
thái; ghi mỗi notebook action cùng đường dẫn ảnh vào
`report/evidence/spark-ui/index.csv` (một dòng cho mỗi notebook action; lặp
Application ID nếu cùng app có nhiều action, ghi tất cả Job ID và ảnh detail do
action đó tạo). Chụp cả lỗi/hủy và ghi
đúng trạng thái;
không trình bày chúng như thành công. Cell local không tạo Spark application
không thuộc yêu cầu này. Tham khảo [Spark Web UI](https://spark.apache.org/docs/latest/web-ui.html)
và [Dataproc Component Gateway](https://docs.cloud.google.com/managed-spark/docs/concepts/accessing/gateways).

## 6. Phase 3 — Data contract và cleaning

NEA xác nhận timestamps là SGT, data có thể thiếu records và chưa qua climate quality control; không coi raw là climate-quality data ([NEA collection](https://data.gov.sg/collections/2279/view)). Xác nhận schema từng file; header mẫu:

```text
date, timestamp, update_timestamp, station_id, station_name, station_device_id,
location_longitude, location_latitude, reading_update_timestamp,
reading_value, reading_type, reading_unit
```

Spark đọc CSV với schema explicit và giữ malformed rows để audit; không `inferSchema` lặp lại trên 8 năm. Giữ original/source_file và reason của quarantined rows.

| Field | Contract |
|---|---|
| Station | ID không null/blank; audit đổi name/device/location |
| Observation time | Parse timezone offset; giữ original; derive local year/date và đối chiếu `date` |
| Interval end | Chỉ tạo sau khi xác minh timestamp/interval semantics và alignment |
| Availability | Dùng update fields làm proxy sau khi xác minh nghĩa; flag missing/invalid/update-before-observation |
| Rainfall | Numeric finite, không âm, `mm`, đúng reading type 5-minute total |
| Coordinates | Finite, lon/lat đúng thứ tự, audit bất thường/relocation |
| Key | `(station_id, interval_end)` unique sau duplicate policy |

Timestamp `08:04:59` cho thấy không được mặc định floor vào bucket 5 phút. Column legends trên [trang 2017](https://data.gov.sg/datasets/d_1990a5a1aeaf3dd243cf4dae294a61c4/view) và [trang 2024](https://data.gov.sg/datasets/d_a0b69d3e02576a1fd0ab673e71f83507/view) hiện không giải thích đầu/cuối interval hoặc nghĩa update fields. Audit seconds/spacing theo năm/trạm và tìm thêm source documentation để xác minh. Alignment tolerance dựa trên evidence, freeze trước model selection, retain raw/offset. Không cộng readings của hai interval khác nhau vì cùng bucket. Semantics chưa rõ thì gate chưa đạt và chưa tạo label chính thức.

Exact duplicates giữ một bản. Conflicting readings cùng key phải quarantine hoặc resolve theo version policy có bằng chứng; không `dropDuplicates` ngẫu nhiên/cộng duplicates. Task 1 có thể dùng canonical final reading; Task 2 history cần bản khả dụng tại prediction time. Final-only export không tái dựng được đầy đủ publication history.

Missing/gap không phải 0. Giá trị cực lớn nhưng finite/nonnegative chưa đủ để loại; rainfall extremes là mục tiêu cần học. Chỉ flag/loại bằng quality rule có nguồn và sensitivity, không winsorize label để giảm độ khó.

Completeness tính trên grid sau alignment: report active station-period và fixed full-calendar denominator; first/last observed không xác nhận commissioned/retired dates. Một ngày đầy đủ có 288 slots, xử lý leap year. Quality summary có rows trước/sau, invalid/conflicts/duplicates/gap lengths, station×month/year coverage, metadata drift. Giữ excluded counts/reasons.

Ghi validated Parquet partition theo year, thêm month nếu profiling cần; tránh station+timestamp partitions gây nhiều file nhỏ. Snapshot có manifest/config/version/quality summary. Các phase sau đọc Parquet; chỉ đọc CSV lại khi cleaning rule đổi.

**Acceptance:** tám năm; schema/unit/time/key đạt; rejected rows có lý do; coverage có mẫu số; interval semantics có evidence.

**Implementation started:** `scripts/run_quality_audit.py` cung cấp hàm để notebook import, đọc đúng paths trong manifest, dùng schema CSV tường minh và giữ raw bất biến. Audit dự kiến xuất `quality_by_year`, `csv_parser_corrupt_candidates`, phân bố `reading_type`/`reading_unit`, duplicate-key groups (có cờ conflict, min/max và số giá trị đọc khác nhau), metadata variations theo station, spacing deviations so với 300 giây và `audit_metadata`. Duplicate/timestamp và spacing hiện chỉ là candidates để điều tra; job không tự deduplicate, quarantine hay gán timestamp thành cuối interval. `update_timestamp` và `reading_update_timestamp` chỉ được kiểm tra parseability/thứ tự, chưa được coi là thời điểm publication đã xác minh. Output bị chặn nếu đặt trong `raw/`.

Mở và chạy `notebooks/01_data_quality.ipynb` trên Jupyter của Dataproc sau khi preflight đạt. Notebook load manifest, xác minh đúng tám GCS objects qua SHA-256, tạo SparkSession trên YARN, gọi `run_audit(...)` trong cell và hiển thị application ID, Spark Job ID(s), trạng thái cuối, output path cùng link Spark Web UI. Không chạy module bằng `spark-submit` hoặc terminal. Mỗi lần chạy có run prefix mới.

**Trạng thái thực thi ngày 11/10/2026:** notebook 00 đã tạo project `sg-rainfall-dsa5208-261011`, liên kết đã xác minh với đúng Education account, bucket private và Dataproc private cluster. Notebook 01 attempt 2 chạy Spark 3.5.3/YARN; cell xác minh SHA-256 byte thực tế khớp manifest cho cả tám CSV. Audit báo 48,538,713 dòng; mọi `reading_type`/`reading_unit` đúng `TB1 Rainfall 5 Minute Total F`/`mm`, manifest scan có 0 dòng sai số field. Action `raw_quality_audit` hoàn tất sau 858.697 giây trong application `application_1791706788393_0001`, Job ID 0–20 SUCCEEDED. Action `deterministic_cleaning` sau đó thất bại: YARN ApplicationMaster timeout và Spark listener OOM. Dù vậy, Parquet `cleaned/` đã ghi `_SUCCESS` và đủ partition 2017–2024 trước khi lỗi. Các output `rejected/`, `quality_by_year/`, `exclusion_counts/`, `alignment_distribution/` và `coverage_and_gaps` chưa được tạo; do đó reconciliation số retained/excluded và coverage toàn tám năm trong Notebook 01 chưa đạt. Không được đánh dấu Phase 3 hoàn tất chỉ dựa trên marker Parquet. Spark UI screenshots chưa chụp; Mac bị khóa khi thực hiện bước review, và API metadata không thay ảnh thật. Cờ corrupt-record chỉ bao phủ lỗi parser; số token thiếu/thừa cần được kiểm soát bằng scan manifest riêng.

## 7. Phase 4 — Task 1: weather patterns

Mỗi analysis ghi mục đích, cohort/denominator, statistic, observation, interpretation và limitation. Không cần dựng bộ câu hỏi nghiên cứu riêng; bám trực tiếp Task 1. Không cộng mọi trạm thành “tổng mưa Singapore”.

| Analysis | Output/phương pháp |
|---|---|
| Completeness | Station×year/month heatmap, gap distribution, exclusions |
| Seasonality | Station-month totals kèm coverage; wet probability = wet valid / all valid; intensity conditional on wet |
| Daily cycle | Month×SGT hour heatmap với denominators và station weighting |
| Across years | Station annual summaries trên cohort chung/coverage tương đương |
| Spatial | Map wet frequency/intensity/extreme rate, tọa độ đã audit, kèm coverage |

Primary comparison dùng equal station weighting trong cohort chung. Coverage cutoff (ứng viên 95%) phải khai báo và chạy sensitivity cutoff/cohort khác; giữ full-cohort descriptive view. Heatmap không chứng minh missingness không gây bias; complete-window filtering có thể mất storm periods.

Observed total ở tháng thiếu data không phải full-month total. Coverage-adjusted estimates cần giả định missingness và comparison với observed totals. Extreme cutoff lấy từ train hoặc external definition có nguồn; training quantile không tự trở thành warning threshold chính thức.

Task 1 mô tả cả 2017–2024. Freeze thiết kế Task 2 trước descriptive exploration 2024 để tránh dùng test-year findings chọn features/thresholds/models. Tám năm chỉ hỗ trợ observed variation, không climate trend/causality mạnh. Export figures + aggregate tables + plotting config; collect aggregates nhỏ.

**Acceptance:** within-year/across-year findings; cohort/denominator/unit/caption/method/limitations rõ; bias sensitivity có evidence.

### 7.1. EDA chi tiết và artifacts bắt buộc

EDA hỗ trợ trực tiếp Task 1; không đặt thêm research questions. Thân báo cáo ưu tiên coverage, distribution, monthly/hourly và annual comparison. Tail, complete-day, station-coordinate và wet-spell panels dùng làm sensitivity/supplementary evidence; đưa appendix khi hữu ích, không kéo dài thân bài bằng các biểu đồ lặp.

| Nội dung | Cách tính và support | Artifact |
|---|---|---|
| Distribution | Theo năm: valid/dry/wet counts, wet fraction, mean valid rainfall, conditional wet mean, wet median/P90/P95/P99, max. Ghi rõ pooled-observation weighting. | `distribution_by_year`, `rainfall_histogram`; `rainfall_distribution.pdf` |
| Tail | Fixed bins cho wet readings; log y để xem tail. Reference = wet P99 của **2017–2022**, giữ nguyên qua các năm. Exceedance denominator = valid readings, strict `>`. Không gọi training P99 là official warning threshold. | `rainfall_tail.pdf` |
| Coverage | Calendar 288 slots/ngày, leap days; zero-observation periods phải có trong denominator. Reference network có thể bao gồm station chưa hoạt động; thiếu operating dates thì không gọi coverage thấp là downtime. | `coverage_summary`, `coverage_heatmap.pdf` |
| Daily sensitivity | Chỉ ngày có đủ 288 unique exact-grid readings. Average daily rainfall theo station-month rồi equal-station average; báo complete/observed và complete/full-calendar station-days. | `daily_completeness`, `complete_daily_patterns.pdf` |
| Month | Equal eligible-station mean của observed total, wet frequency, conditional wet intensity; primary station-month coverage ≥95%. Support stations và mean coverage đi cùng số liệu. | `monthly`, `monthly_frequency_intensity.pdf` |
| Hour | SGT **interval-end hour**; coverage tháng **và nhóm giờ** mỗi station-month ≥95%. Expected slots cho mỗi giờ = 12×days. Audit bao gồm giờ không có observation; không thay missing rainfall bằng 0. Equal station-year-month-hour weighting trong heatmap; yearly curves equal station-month weighting. | `hourly_coverage`, `hourly_by_year`, `monthly_hourly_patterns.pdf`, `hourly_by_year.pdf` |
| Annual comparison | Cùng station IDs trong mọi năm; fixed cutoffs 90/95/99%. Xuất số stations kể cả cohort rỗng, observed total và wet frequency. Full observed changing-cohort view riêng. | `annual_cohort_support`, `annual_common_cohort`, `annual_descriptive`, `annual_patterns.pdf` |
| Stations | Chỉ plot station có coordinates không đổi trong toàn period; annual coverage ≥95%; equal eligible-year average. Ghi eligible years/valid slots; không nội suy diện tích hoặc suy luận nhân quả. | `station_spatial_summary`, `station_map`, `station_spatial_patterns.pdf` |
| Wet spells | Positive readings liên tiếp đúng 300 giây cùng station. Duration = 5×records phút; gap/boundary-censored tách riêng. Count và duration median/P90/P99/max theo năm bắt đầu. Không coi gaps là dry hay sequences là meteorological storms. | `wet_spell_summary`, `wet_spell_patterns.pdf` |

**Quy ước thời gian đã được người dùng chấp nhận:** timestamp là cuối interval 5 phút; đây là giả định của project, chưa phải xác nhận từ NEA. Calendar month/day/hour ở EDA nhóm theo timestamp cuối interval. Tổng được gắn nhãn calendar period của timestamp, không tự nhận là meteorological daily total đã xác minh.

**Trình tự và output Jupyter:** `02_task1_weather_patterns.ipynb` thực hiện EDA 2017–2023; `05_final_evaluation.ipynb` thực hiện EDA đủ 2017–2024 sau freeze Task 2. Spark materialise aggregates một lần, sau đó plot/export từ các bảng nhỏ; driver collect tối đa 20,000 rows mỗi bảng, không collect raw records. Notebook hiển thị tables/figures/findings; JSON trên GCS và CSV + PDF/PNG giữ full evidence kể cả khi `.show(20)` rút gọn display. Figures dùng SciencePlots, không đòi cài TeX trên worker.

`findings.json` phải chứa actual peak/trough, units, support và common-cohort availability, tính từ aggregates; chưa chạy thì không điền finding minh họa. Sau mỗi action phải review **Spark Web UI / History Server Jobs summary và từng Job detail**, chụp ảnh thật có application/job IDs, status, duration, stages/tasks. Event metadata chỉ là index hỗ trợ đối chiếu, không thay screenshots.

**Acceptance EDA:** ít nhất một within-year finding, một annual comparison có sensitivity (hoặc lý do không có common cohort), và một coverage/distribution finding, tất cả có số liệu thật. Nêu peak–trough theo mm hoặc percentage points, mẫu so sánh, support và giới hạn; không kết luận chỉ từ màu biểu đồ. Nếu thiếu tháng/giờ, ghi represented months/hours, không coi là zero hoặc tuyên bố đã so sánh đầy đủ.

**Trạng thái thực thi 11/10/2026:** Notebook 02 attempt 1 dừng trước EDA action do SciencePlots chưa có trong `sys.path`; application `application_1791706788393_0002` không tạo Spark job. Đã sửa cài wheel vào thư mục vendor của project và chạy lại Notebook 02 attempt 2 trên Parquet `cleaned/`: action `task1_pretest_eda` trong application `application_1791706788393_0003` hoàn tất sau 498.084 giây, cả 241 Spark jobs SUCCEEDED. Notebook có output; CSV/JSON và PDF/PNG được lưu trong `report/tables/pretest/` và `report/figures/pretest/`. Kết quả cho 2017–2023: 84 station IDs trong fixed network; mean network coverage mỗi năm 39.4%–80.8%; không có common station cohort ngay cả ở cutoff 90%; peak tháng khác nhau theo năm, và 15:00 SGT là giờ có wet frequency cao nhất trong 6/7 năm. `report/main.tex` đã được bổ sung kết quả, mẫu số và giới hạn diễn giải. So sánh đủ tám năm phải chạy sau khi Task 2 freeze. Ảnh Jobs summary và 241 Job details từ Spark History UI chưa được lưu thành tệp vì Mac đang khóa; đây là acceptance item còn mở, History API không được xem là thay thế. Notebook 03 hoàn tất: app `application_1791706788393_0004`; action fixture mất 47.949 giây, 18 jobs thành công và cả 7 leakage/target fixtures pass; action `base_features_and_labels` mất 754.368 giây, Job ID 18–26 đều SUCCEEDED. Parquet `base_features/` và `sample_attrition/` có `_SUCCESS`. Eligible base station/time keys: train 32,798,917; selection 3,325,499; calibration 3,393,373; test 2024 6,374,476. Mỗi split invalid do future window thiếu lần lượt 814,899; 11,350; 11,884; 31,408. Các số này là window-completeness counts, không phải positive prevalence. Notebook đã materialize sáu future sums cho partition 2024 trước freeze, nhưng chỉ `label_valid` counts được hiển thị; chưa xem/dùng 2024 prevalence, model scores hay metrics. Đây là giới hạn quy trình phải giữ minh bạch, không gọi test hoàn toàn blind. 2024 partition không được đọc cho scoring đến khi frozen manifest được ghi. App4 Jobs summary đã mở trực tiếp trong Spark History Server và cho thấy 27 completed jobs; trang chi tiết Job 20 cho thấy SUCCEEDED, 9.6 phút, stage hoàn tất với 29/29 tasks. Hai ảnh đã được chụp và hiển thị trong interaction nhưng chưa xuất thành tệp trong `report/evidence/`. Summary file và toàn bộ 27 Job detail files vẫn pending.

## 8. Phase 5 — Target và inference contract

Sau khi xác minh semantics, `r(s,u)` là tổng mưa interval `(u−5min,u]`; `t` là decision time trên grid 5 phút SGT:

\[
R_{30}(s,t)=\sum_{k=1}^{6}r(s,t+5k\text{ min}),
\qquad Y(s,t,\tau)=\mathbf{1}[R_{30}(s,t)>\tau].
\]

Giữ strict `>`: bằng threshold là negative. Numeric precision theo resolution thực tế để tránh lỗi equality. Label là tổng mưa tương lai, không chỉ reading tại `t+30`, không gồm interval kết thúc tại `t`.

Label valid cần đúng sáu interval khác nhau, aligned/contiguous, rainfall hợp lệ. Kiểm tra timestamps, không chỉ count. `lead(1..6)`/`rowsBetween(1,6)` trên data có gap có thể lấy readings xa hơn 30 phút. Dùng explicit grid hoặc time-range theo epoch seconds + completeness checks. Trên grid đúng, label range `[300,1800]`, past-30min range `[−1500,0]`; history còn cần availability filtering. Range boundaries inclusive ([Spark window API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Window.rangeBetween.html)).

Prediction grid có policy rõ, kể cả khi current reading thiếu; nếu loại những time này, report mất coverage. Không fill/interpolate future rainfall. Báo sample attrition theo split/station/month cho future gaps, history và boundary purge.

`predict.py` nhận `station_id`, `prediction_time`, `threshold_mm`; tự lấy available history/metadata, build cùng features và trả probability. Status rõ cho unseen station/history thiếu/invalid time/out-of-domain threshold. Off-grid time reject, không âm thầm round làm đổi horizon.

Grid ứng viên `0.2,0.5,1,2,5,10 mm` chỉ là training design; chốt grid/domain từ **train** theo resolution/positive support, lưu config. Threshold là numeric feature. API nhận numeric thresholds trong domain; đánh giá thêm thresholds ở giữa chưa dùng train, không hứa extrapolation ngoài domain. Climatology dùng training empirical survival để xử lý arbitrary threshold.

Tính base features/label một lần cho `(station,t)`, sau đó expand grid. Threshold copies thuộc cùng split/sampling group. Ghi base samples/expanded rows và label monotonicity; copies không độc lập.

**Acceptance:** correct horizon/strict inequality/completeness/domain; fixtures dry/wet/gap/duplicate/equality/off-grid; inference trả probability.

## 9. Phase 6 — Features và availability leakage

Feature reading phải có **cả** `interval_end ≤ t` và `available_time ≤ t`. Observation cũ chưa chắc published tại `t`; mẫu local có delay nên `rain_t` không mặc định available.

Xác minh nghĩa update fields trước as-of lookup; không dùng later revisions. Nếu chỉ có final revision, ghi giới hạn và dùng conservative proxy/delay sensitivity dựa trên train. Nếu availability chưa xác minh được, gọi kết quả là **retrospective event-time prediction** giả định observation tức thời; không claim real-time performance. Availability-aware sensitivity bổ sung khi metadata cho phép. Mode/assumptions phải frozen trong results/model metadata.

| Category | Features ứng viên |
|---|---|
| Station | ID, time-valid coordinates, unseen handling |
| Temporal | Fractional-hour sin/cos; day-of-year sin/cos với period của năm; month |
| History | Aligned lags 5/10/15/30/60 min; totals 15/30/60/180 min; max/mean/wet count |
| Missingness | Valid count/coverage của window, missing indicators, latest-reading age |
| Query | Numeric `threshold_mm` |
| Optional spatial | Neighbour mean/max/wet count + valid-neighbour count, as-of filtered |

Lags theo elapsed time, không row position. Rolling boundaries ghi rõ. Impute history bằng rule fit train + missing indicators, không backfill tương lai. Build windows với context xuyên năm/tháng; không mất history vì processing riêng từng partition. Chỉ label phải nằm hoàn toàn trong split.

Metadata/neighbour locations phải khả dụng tại time tương ứng; không chọn neighbours dựa trên correlation 2024. Indexer/scaler/imputer/feature selection và learned statistics chỉ fit train của fold.

Leakage fixtures: sửa rainfall/update/metadata sau `t` không đổi features tại `t`; observation cũ published sau `t` bị loại trong availability-aware mode; label dùng future; windows không nhảy qua gap/mix station. Deterministic rerun và key uniqueness cũng phải kiểm tra.

**Acceptance:** feature dictionary có source/window/availability/missing rule; fixtures chứng minh các time constraints.

## 10. Phase 7 — Temporal split, tuning và calibration

| Subset | Prediction times | Vai trò |
|---|---|---|
| Train | 2017–2022 | Fit preprocessing/models; threshold design/sampling |
| Model-selection set | Jan–Jun 2023 | Chọn features/hyperparameters/model, cutoff, calibration recipe |
| Calibration holdout | Jul–Dec 2023 | Fit calibrator sau base model freeze |
| Final test | 2024 | Đánh giá frozen system |

Tách nửa năm 2023 tránh fit calibrator và report performance trên cùng samples; ghi seasonal limitations. Nếu dùng temporal folds chọn calibration recipe, giữ order/purge. Không tune lại base model từ calibration holdout.

Với split `[start,end)`, giữ `t ≥ start` và `t+30min < end` vì endpoint `end` thuộc split kế tiếp. Purge cuối 2022, giữa 2023, cuối 2023 và cuối 2024 thiếu horizon; audit prediction/label endpoints. History trước `start` là thông tin quá khứ hợp lệ. Nếu claim simulation tại training cutoff thật, training labels còn phải available trước thời điểm fit; historical final revisions không tự đáp ứng điều này.

Không random CrossValidator/TrainValidationSplit mặc định. Optional temporal folds: 2017–2020→2021, 2017–2021→2022, rồi selection đầu 2023. Threshold copies/stations cùng time block giữ cùng fold.

Tuning cap trước khi chạy: LR tối đa 4 regularization configs, GBT tối đa 4 depth/iteration configs trên deterministic training pilot của variant B. Chọn hyperparameters bằng selection period rồi fit cả A và B trên cùng tập train, dùng cùng settings để so sánh ablation matched. Ghi số candidate/runtime/cost; nếu budget buộc subsample, ghi years/cohort/seed/fraction/actual coverage, không gọi là full-data training.

Mặc định base model fit 2017–2022, selected bằng đầu 2023, calibrator fit cuối 2023. Không refit base model trên toàn bộ 2023 mà giữ calibrator cũ; refit cần temporal out-of-fold calibration riêng.

Freeze trước test: manifests, cleaning/alignment/availability, domain/grid, features, split cutoffs, sampling/seed, pipeline/hyperparameters/calibrator/cutoff, selection objective/metric settings và Git commit. Changes sau khi xem 2024 là post-test exploration phải khai báo.

**Acceptance:** no horizon crossing; learned transforms đúng train; calibration holdout riêng; frozen manifest trước test.

## 11. Phase 8 — Models và distributed execution

Ba models chính: climatology, Spark MLlib LogisticRegression, Spark MLlib GBTClassifier. Climatology = training empirical survival theo station/month/hour, smoothing/support policy và fallback về nhóm rộng/global cho sparse/unseen cases; support/smoothing chọn trên model-selection set, lưu lookup.

LR: StringIndexer (`handleInvalid=keep`) → OneHotEncoder station → imputation/assembly → scaling phù hợp → LogisticRegression. Giữ sparse vectors, không center one-hot gây densification. GBT không cần scaling; indexed categorical station phải giữ metadata và `maxBins ≥ categories`, tính unseen bucket; one-hot là phương án khác ([GBT API](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.classification.GBTClassifier.html)).

Save **PipelineModel** gồm learned preprocessing. Unseen `keep` tránh lỗi encoding, chưa chứng minh generalization; report counts/fallback. Label mapping cố định 0=no exceedance, 1=exceedance; export `probability[1]`, không nhầm class prediction với xác suất.

Mặc định natural prevalence. Nếu oversample/class weights, ghi policy và ảnh hưởng probability; calibration/evaluation dùng natural validation/test prevalence. Không rebalance test.

Spark SQL/DataFrame cho features/windows/metrics; `spark.ml` cho training. API links trong plan trỏ tới `latest`; khi implement phải đối chiếu documentation của Spark version đã pin trên cluster. Không collect/toPandas toàn bộ raw/features lên driver. Collect aggregates/curves có giới hạn. Base features trước threshold expansion; persist reused data có lợi, unpersist khi xong; tránh repeated CSV scans/Python UDF thay phép toán SQL có sẵn.

Ghi rows/bytes/partitions, stage runtime và executor evidence; inspect window/join plan/skew. Station-only windows có thể gom nhiều năm vào task lớn. Nếu chunk theo thời gian, thêm history halo 180 phút/future halo 30 phút rồi trim ownership; không tính windows trên chunk thiếu context. Chọn file/shuffle sizes bằng profiling, không giả định một setting tối ưu.

**Acceptance:** ba models dùng same held-out cohort; MLlib pipelines chạy; probabilities finite và trong [0,1]; distributed/runtime/cost evidence có lưu.

## 12. Phase 9 — Metrics, calibration và scientific comparisons

Predeclare selection objective: mean PR-AUC của primary thresholds có đủ classes trên model-selection set; chọn thresholds từ train support, không loại vì test khó. Với scores gần nhau trong tolerance khóa trước, ưu tiên Brier thấp rồi model đơn giản/ít tốn hơn. Nếu metric undefined trên model-selection set, report và dùng fallback rule khóa trước test. Score của candidate được chọn có xu hướng lạc quan sau lựa chọn và không phải unbiased validation estimate. Ranking tốt chưa bảo đảm probability tốt.

| Output | Quy tắc |
|---|---|
| PR-AUC | Spark BinaryClassificationEvaluator `areaUnderPR` trên score/probability; ghi numBins |
| ROC-AUC | Same cohort; accuracy không là primary metric |
| Brier | Spark mean `(p-y)^2`; baseline/Brier skill nếu denominator > 0 |
| Log loss | Clip bằng epsilon frozen, Spark aggregation |
| Calibration | Frozen validation bins; mean p, observed frequency, count mỗi bin |
| Precision/Recall/F1 | Probability cutoff chọn trên model-selection set, tách khỏi rainfall threshold tau |
| Support | Base/expanded counts, positives/negatives/prevalence/station-time coverage |

Spark PR-AUC không được gọi là Average Precision nếu chưa tính đúng định nghĩa. Ghi version và curve approximation; `numBins=0` tắt downsampling khi cost cho phép. Single-class nhóm ghi AUC N/A; rare groups vẫn nằm trong bảng với support/uncertainty ([evaluator API](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.evaluation.BinaryClassificationEvaluator.html)).

Report per-threshold, macro weighting frozen, station/month strata; pooled thresholds chỉ phụ vì dễ che hiệu năng từng ngưỡng. PR curve có prevalence reference; không khẳng định Spark area của constant predictor luôn bằng prevalence.

Calibration assessment bắt buộc, correction khi validation cho thấy cần. Chọn recipe bằng train/selection temporal predictions, fit trên calibration holdout; Spark IsotonicRegression hoặc sigmoid mapping nếu support phù hợp. Per-threshold mapping cần support/fallback và handling unseen thresholds. Lưu raw/calibrated outputs, mapping/domain/fallback; không chọn bins/cutoff hoặc fit mapping trên test.

Monotonicity assessment bắt buộc: cùng station/time/history, `tau1 < tau2` ⇒ probability không tăng. Ghi violation rate/max excess với frozen numeric tolerance trước/sau calibration. Violation nghĩa là chưa có survival function nhất quán. Projection/interpolation correction là extension phải freeze/test/lưu trong inference pipeline, không chỉ sửa plot.

Ablation A=station+time+threshold; B=A+history+missingness là chính; C=B+neighbours optional. Cùng samples/folds/grid/tuning budget; nếu C mất samples thì so sánh intersection và report coverage separately. Báo A/B ablation trên model-selection set; final holdout một lần so climatology, LR-B và GBT-B trên cùng eligible cohort. Error analysis theo month/station/threshold/unseen, onset/continuation định nghĩa từ available history tại `t`.

Observations chồng horizon và storms phụ thuộc theo thời gian. Confidence intervals dùng paired day/week block bootstrap, resample all stations/thresholds cùng block; block length chọn từ train/validation trước test. Không bootstrap expanded rows độc lập. Nếu budget hạn chế, ưu tiên Brier difference uncertainty, ghi metric nào chỉ có point estimate.

**Acceptance:** comparison/threshold tables, calibration, monotonicity, A/B ablation/error analysis; support, excluded samples và uncertainty rõ.

## 13. Phase 10 — Artifacts, LaTeX và submission

Model package gồm PipelineModel, calibrator, climatology lookup/fallback, feature schema/config, threshold domain, availability assumptions, training manifest, versions/Git commit và prediction entry point. Download cả Spark model directory tree, không coi là một pickle. Có sample queries/expected probabilities/status cho unseen/missing/out-of-domain.

Load trong fresh session cùng runtime, inference từ public station/time/threshold contract, compare saved fixtures trong frozen tolerance. Fixtures phải chứa một history/metadata sample nhỏ đủ chạy offline; dữ liệu thật lấy từ input path do người chạy cấu hình, không bắt buộc bucket cá nhân còn tồn tại. Save thành công chưa chứng minh model reproduce được.

### 13.1. Trọng tâm và hình thức báo cáo

**Bám trực tiếp hai tasks trong đề Project 2.** Không bắt buộc đặt research questions, hypotheses hay xây dựng khung nghiên cứu riêng. Báo cáo giải thích dữ liệu đã được xử lý thế nào, Task 1 tìm thấy patterns gì và Task 2 dự đoán xác suất tốt đến đâu. Mẫu Project 1 chỉ dùng để tham khảo hình thức LaTeX và cách trình bày bằng chứng; không chuyển nội dung nghiên cứu MongoDB sang bài này.

Viết báo cáo bằng tiếng Anh rõ ràng, câu ngắn, thuật ngữ nhất quán. Dùng **LaTeX và mẫu Project 1** sau khi tìm source template. Đã kiểm tra thư mục Project 1 và submission ZIP: có PDF nhưng chưa tìm thấy `.tex/.cls/.sty`. Nếu source chưa được cung cấp, dựng bố cục dựa trên PDF và ghi rõ đây là bản tái dựng. Giữ A4, một cột, font serif dễ đọc, section numbering, captions và cross-references. Không cần sao chép toàn bộ front matter của Project 1; chỉ thêm mục lục/list of figures khi giúp đọc bài. Project 2 làm cá nhân: trang bìa và submission chỉ ghi một tên và một student ID; không sao chép danh sách thành viên Project 1.

Plot dùng **SciencePlots** (import `scienceplots`) + Matplotlib, vector PDF khi phù hợp; kiểm tra TeX dependency của style. Khi viết bản báo cáo thực tế phải đọc skill **academic-writing**. Skill đã được người dùng cung cấp tại `data/academic-writing-SKILL.md` và đã đọc ngày 11/10/2026. Áp dụng cách viết theo bằng chứng, không tự tạo kết quả hoặc novelty claim. Khung hiện có ở `report/main.tex`, hướng dẫn điền ở `report/OUTLINE.md`.

Chưa có rubric chấm điểm chi tiết ngoài yêu cầu trong `project_description.txt`; không tự gán trọng số điểm. Gợi ý **10–12 trang nội dung chính**, không phải page limit chính thức; references, AI declaration và appendix tính riêng. Điều chỉnh theo kết quả thực tế, không thêm hình/bảng chỉ để đủ trang.

| Phần | Nội dung phải trả lời | Dung lượng gợi ý |
|---|---|---|
| Introduction | Bài toán, dữ liệu 2017–2024, hai nhiệm vụ và phạm vi | 0.5 trang |
| Dataset and preprocessing | Nguồn, schema, thời gian, cleaning và dữ liệu còn lại | 1–1.5 trang |
| Computing environment | Cấu hình thực chạy và cách dùng Spark/MLlib | 0.5 trang |
| **Task 1: Rainfall patterns** | Within-year và across-year patterns, hình và diễn giải | **2.5–3 trang** |
| **Task 2: Probability prediction** | Target, features, split, models, đánh giá xác suất và lỗi | **3–4 trang** |
| Discussion and limitations | Ý nghĩa kết quả, giới hạn của dữ liệu và mô hình | 0.5–1 trang |
| Reproducibility and conclusion | Cách chạy lại, artifacts và kết luận hai tasks | 0.5 trang |

Abstract khoảng 150–200 từ, viết cuối: mục tiêu, dữ liệu/phương pháp, một kết quả Task 1 và một kết quả Task 2 có số liệu thật, giới hạn chính. Không điền số liệu minh họa vào bản nộp.

### 13.2. Dataset, cleaning và môi trường chạy

Đủ ngắn để người đọc hiểu nguồn gốc các kết quả, nhưng phải nêu các quyết định ảnh hưởng đến kết luận:

- Nguồn dữ liệu, giai đoạn sử dụng, đơn vị rainfall, station identity và timezone. Giải thích observation timestamp khác update timestamp; ghi bằng chứng hoặc giả định về interval/availability.
- Bảng theo năm: raw rows, rows sau cleaning, số stations, valid station-days/coverage và số observations bị loại theo lý do. Phân biệt missing, invalid, exact duplicate và conflicting records; đếm loại bỏ theo thứ tự để tránh cộng trùng.
- Giải thích vì sao không thay missing bằng 0, cách xử lý gaps/revisions và station metadata. Nêu ảnh hưởng đến Task 1 và số samples có đủ history/future cho Task 2.
- Cấu hình **thực chạy**: region, master/workers, machine type, vCPU/RAM, disks, Dataproc image, Spark/Python/Java versions, executor settings quan trọng. Có thời gian xử lý/training và evidence Spark jobs chạy trên workers; không dùng cấu hình dự kiến làm kết quả.
- Với **mỗi lệnh hoặc notebook action chạy Spark**, đối chiếu application trên Spark Web UI sau khi các job kết thúc; dùng live UI nếu application còn mở, History Server nếu đã kết thúc. Lưu ảnh Jobs summary và từng Job detail, mã Application ID/Job ID, trạng thái cuối, thời lượng và stages/tasks vào chỉ mục evidence. Ghi rõ nếu ứng dụng/job thất bại hoặc không có event log để khôi phục UI.
- Mô tả flow raw CSV → cleaned Parquet → aggregates/features → MLlib → metrics/artifacts. Log dài, Spark plans và toàn bộ config đưa appendix hoặc repo.

### 13.3. Task 1 — Viết kỹ patterns và cách diễn giải

Phần này phải đáp ứng riêng cả **within-year** và **across-year**, theo Phase 4. Trình bày theo thứ tự:

1. **Coverage context:** hình heatmap station/year hoặc year/month và bảng quality. Cho biết tập stations/ngày hợp lệ dùng trong các hình tiếp theo; coverage thấp có thể làm tổng rainfall thấp giả tạo.
2. **Monthly patterns:** mức rainfall và wet frequency theo tháng với denominator/coverage rõ. Chọn aggregation phù hợp ở Phase 4; không cộng rainfall của nhiều stations rồi gọi là tổng rainfall của Singapore. Nêu tháng/giai đoạn cao thấp bằng số liệu, độ nhất quán giữa năm và ảnh hưởng missingness.
3. **Hourly patterns:** rainfall/wet frequency theo giờ SGT trên observations hợp lệ. Nêu giờ có mưa thường xuyên hoặc lượng mưa lớn hơn và độ khác biệt thực tế; tránh suy luận nguyên nhân chỉ từ hình.
4. **Across-year patterns:** so sánh 2017–2024 bằng tập stations có coverage tương đương/common cohort. Nêu năm cao thấp và kiểm tra kết luận có đổi khi đổi coverage policy/cohort; không khẳng định xu hướng khí hậu dài hạn từ tám năm.
5. **Spatial differences nếu hữu ích:** map hoặc station summary có units/support; phân biệt vị trí với thay đổi mạng lưới stations. Đưa appendix nếu không bổ sung insight cho yêu cầu chính.

Bổ sung một đoạn **distribution** trước monthly patterns: tách valid dry/wet proportions và conditional wet intensity/quantiles, ghi pooled weighting. Dùng complete-day và wet-spell outputs để giải thích sensitivity hoặc short-term persistence khi có evidence; giữ spatial/tail detail ở appendix nếu không cần cho finding chính. Chi tiết thống kê và artifacts theo mục 7.1.

Mỗi đoạn kết quả gồm: **quan sát có số liệu → cách tính/cohort → diễn giải → giới hạn liên quan**. Dùng tên hình/bảng để dẫn chứng; không chỉ mô tả màu hoặc lặp caption. Nếu đối chiếu mùa mưa/monsoon, dẫn nguồn khí tượng chính thức và tách thông tin nền khỏi kết quả được tính từ dataset. Không gán causal explanations chưa có dữ liệu chứng minh.

### 13.4. Task 2 — Viết kỹ từ target đến kết quả xác suất

**Problem and target.** Nêu input station/time/threshold, output xác suất tích lũy rainfall trong 30 phút kế tiếp **lớn hơn** threshold. Có công thức `P(R30 > tau | available history)` và một timeline ngắn giải thích sáu intervals tương lai. Nêu đơn vị, boundary convention, điều kiện đủ label/history, threshold domain và xử lý missing/unseen/out-of-domain. Khai báo giả định availability nếu chỉ làm retrospective prediction.

**Features and split.** Bảng features chỉ liệt kê features thực dùng: nhóm thời gian/station/threshold, rainfall history, missingness; mỗi nhóm có cửa sổ, ý nghĩa và lý do chọn. Giải thích ngắn cách tránh nhìn tương lai. Bảng split ghi train 2017–2022, model-selection set Jan–Jun 2023, calibration Jul–Dec 2023 và final test 2024, kèm base samples, threshold-expanded samples, positives/prevalence và purge policy. Không dùng test để chọn model/features/calibration.

**Models and training.** Giải thích climatology baseline, MLlib LR và GBT bằng vài câu cho mỗi model; nêu preprocessing, tuning budget, actual selected hyperparameters, selection criterion và calibration recipe. Config đầy đủ ở appendix/repo. Phân biệt rainfall threshold `tau` với probability cutoff dùng precision/recall. Nêu cách xuất probability của class 1 và lưu đầy đủ pipeline.

**Main results.** So sánh ba models trên cùng final-test cohort. Bảng chính có PR-AUC, ROC-AUC và Brier; log loss/cutoff metrics bổ sung khi giúp diễn giải. Bảng per-threshold có sample count, positives, prevalence và metrics của baseline/model đã chọn. Nêu rõ macro/pooled aggregation, AUC N/A với single-class nhóm và support nhỏ. Nếu bảng nhiều thresholds quá dài, giữ primary thresholds trong thân bài và đưa bảng đầy đủ appendix.

**Probability quality.** Có reliability diagram với predicted probability, observed frequency và counts mỗi bin; so sánh raw/calibrated khi correction đã thực hiện. Giải thích ranking và calibration bằng kết quả thực tế: model phân biệt mưa tốt không tự đồng nghĩa xác suất đáng tin. PR curves dùng vài primary thresholds đã khóa trước test, có prevalence/support và model labels rõ.

**Features and errors.** Tóm tắt A/B ablation để cho thấy history/missingness đóng góp thế nào. Phân tích ngắn các lỗi chính theo threshold, station/month, onset/continuation và unseen khi có samples; chọn vài kết quả có ý nghĩa thay vì đưa mọi strata vào thân bài. Báo cáo monotonicity violation và giới hạn khi suy diễn xác suất cho threshold chưa học. Uncertainty tính theo Phase 9; không gọi khác biệt nhỏ là statistically significant nếu chưa có bằng chứng phù hợp.

**Kết luận Task 2.** Trả lời trực tiếp model nào được chọn trên model-selection set và final test đạt gì, probability có đáng tin ở điều kiện nào và thất bại ở đâu. Không gọi model-selection scores là unbiased validation estimates. Nếu ML không hơn climatology hoặc calibration không cải thiện, báo cáo đúng kết quả và giải thích trong giới hạn evidence; không sửa scope/cohort để làm model thắng.

### 13.5. Figures, tables và bằng chứng cho báo cáo

Kế hoạch dưới đây hướng dẫn chọn outputs; không phải quota bắt buộc:

| Artifact | Nội dung và nguồn dữ liệu |
|---|---|
| `quality_by_year.csv` / Table: data quality | Spark cleaning audit, attrition và coverage theo năm |
| `coverage_heatmap.pdf` | Aggregated valid coverage; làm context cho Task 1 |
| `monthly_hourly_patterns.pdf` | Hai panels monthly/hourly; units, valid denominators và cohort |
| `annual_patterns.pdf` | So sánh năm trên common cohort; sensitivity nếu cần |
| Table: environment / features | Actual run metadata và frozen feature config |
| Table: model comparison / threshold results | Frozen 2024 metrics, same cohort, support và uncertainty có sẵn |
| `pr_curves.pdf` / `calibration.pdf` | Spark score aggregates; primary thresholds, bin counts và split |
| Table: ablation / error summary | A/B same samples và vài failure strata; bảng dài ở appendix |
| `report/evidence/spark-ui/` + `index.csv` | Ảnh Spark History Server sau từng lệnh/application Spark: Jobs summary và chi tiết mọi Job ID do lệnh tạo; Application ID, Job ID(s), trạng thái cuối, thời lượng, stages/tasks và đường dẫn ảnh |

Mọi caption phải tự hiểu được: thời gian/split, cohort, units, statistic, missingness/coverage rule và model nếu liên quan. Axis có units/labels, legend đọc được khi in A4; tránh quá nhiều đường trên một panel. Dẫn hình/bảng trong phần văn bản trước hoặc gần vị trí xuất hiện.

Spark export **aggregate CSV/JSON nhỏ** cho tables/curves; không kéo toàn bộ raw/features vào Python để vẽ. Mỗi output lưu `RUN_ID`, split/cohort/config và nguồn artifact. Tạo table/numeric macros LaTeX từ outputs đã freeze để tránh chép tay sai; dùng manifest đơn giản nối figure/table với output và commit. Một metric xuất hiện ở abstract, bảng và conclusion phải khớp giá trị, rounding và cohort.

Mỗi notebook action tạo Spark job được ghi thành một dòng trong
evidence index. Sau khi các job của action kết thúc, mở **Spark Web UI** (live
UI nếu application còn mở, Spark History Server nếu application đã kết thúc;
không dùng Dataproc Jobs/YARN page), kiểm tra Jobs summary và trang chi tiết của
mọi Spark Job ID do action tạo rồi chụp ảnh màn hình. Lưu ảnh gốc dưới
`report/evidence/spark-ui/<RUN_ID>/`; appendix liệt kê Application ID, Job ID(s),
command/action, trạng thái cuối, thời lượng và đường dẫn ảnh, đồng thời chèn ảnh
tương ứng để người đọc xem lại.
Nếu một application không tạo Spark Job ID, ghi rõ và giữ ảnh summary. Nếu chưa
có kết quả chạy, để rõ chưa thực hiện; không tạo ảnh minh họa thay bằng chứng thật.

### 13.6. Discussion, reproducibility và quy trình hoàn thiện

Discussion tổng hợp hai tasks: patterns giúp hiểu rainfall và lựa chọn features ra sao; model tận dụng history tốt tới đâu; kết quả giới hạn bởi rainfall-only inputs, missing/revisions, short record, rare heavy rainfall, availability assumptions, threshold domain và unseen stations thế nào. Chỉ đưa limitations thật sự áp dụng. Không dùng các từ “best”, “robust”, “real-time” hoặc “significant” vượt phạm vi đã kiểm chứng.

Reproducibility dẫn README, exact runtime/config/commit, manifest, model package và lệnh load/predict. Conclusion tóm tắt một hoặc hai findings chính của mỗi task và giới hạn sử dụng; không đưa kết quả mới. References gồm nguồn dữ liệu và tài liệu/phương pháp đã dùng; citation nào cũng phải có nguồn thật, không placeholders.

AI declaration ghi đúng công cụ/model và công việc được hỗ trợ (planning/code/debugging/analysis/drafting/editing), trách nhiệm cá nhân theo mẫu đề; không claim mọi kết quả verified khi chưa kiểm tra. PDF có names/student IDs/references/captions.

Quy trình viết:

1. Viết dataset/cleaning/environment/target/split ngay khi đã xác nhận, song song với xử lý dữ liệu; chưa có kết quả thì để draft rõ, không đoán số.
2. Hoàn thiện Task 1 từ aggregates, Task 2 từ frozen evaluation outputs; viết diễn giải sau mỗi hình/bảng.
3. Viết discussion/conclusion/abstract cuối; bỏ đoạn lặp và phần lý thuyết không phục vụ đề.
4. Compile LaTeX, sửa references/overflow; render và đọc toàn bộ pages ở kích thước A4. Kiểm tra figures, font, captions, table widths và ngắt trang.
5. Đối chiếu từng yêu cầu đề với section/page: cleaning, within/across-year patterns, features, model, environment/VM config, performance, limitations/insights và AI declaration. Kiểm tra số liệu với outputs, không còn TODO/placeholder/broken references trong bản nộp.
6. Đối chiếu từng lệnh/notebook action Spark với live Spark Web UI hoặc History Server và evidence index: mỗi action có ảnh Jobs summary; mỗi Job ID có ảnh detail, ID/trạng thái khớp log và output; không thiếu lần chạy thất bại/bị hủy.

README: prerequisites/config mẫu, data nguồn/manifest/upload, notebook order, batch commands, versions/lifecycle/artifact locations và load/predict. Package tự chứa source và model cần chấm; không phụ thuộc bucket cá nhân còn truy cập được.

```text
DSA5208_Project2_StudentID/
├── README.md
├── requirements-local.txt
├── requirements-cluster.txt
├── config/                 # examples + experiment config, không credential
├── notebooks/
├── src/
├── scripts/
├── tests/
├── models/                 # pipeline/calibrator/lookup + metadata
└── report.pdf
```

LaTeX source, figures và aggregate tables giữ trong repo để reproduce report. Không đóng gói raw/cache/temp output/credentials. Kiểm tra Canvas size limit, giải nén/load model từ chính submission archive.

**Acceptance:** PDF đầy đủ yêu cầu; README chạy được; archive tự chứa model/code/config/fixtures cần thiết; fresh-session reload và inference thành công.

## 14. Phase 11 — Shutdown, final QA và timeline

Trước shutdown: export notebooks, persist processed/features/model/results/run manifest trên GCS và đối chiếu artifacts. Sau đó dùng cell shutdown trong `00_environment.ipynb` để xóa đúng cluster do run này tạo và truy vấn lại danh sách cluster trong project/region; giữ output xác nhận trong notebook.

Xác nhận cluster biến mất, kiểm tra remaining VM/disk/tài nguyên phụ/billing. GCS còn storage charges: download và đối chiếu model/results/submission về local trước khi xóa bản cloud không còn cần; đặt retention trong credit budget và trước expiry, không giữ bucket vô thời hạn. Không thấy notebook chưa chứng minh compute đã xóa.

Final checklist:

- [ ] Billing account ID thực đã xác minh chứa credit `3LU63BPGY77JFDQQ`; project chỉ link account này, không dùng credit/billing khác.
- [ ] Credit ledger, pending costs và remaining balance đã kiểm tra; không có paid fallback khi credit không đủ/hết hạn.
- [ ] Manifest chỉ 2017–2024, hashes và paths reproducible.
- [ ] Manifest bytes/rows/schema/timestamp đã đối chiếu; retrieval time gốc còn unknown được khai báo rõ.
- [ ] Schema/interval semantics/availability/quality policy có evidence.
- [ ] Không missing=zero; duplicate/conflicts/gaps deterministic và audit được.
- [ ] Label đúng 30 phút/strict greater-than, không vượt split/fold.
- [ ] Learned preprocessing/model/calibration fit đúng temporal subsets.
- [ ] Task 1 có within-year/across-year findings và coverage/bias sensitivity.
- [ ] Inference nhận station/time/threshold; domain/status rõ.
- [ ] Climatology/LR/GBT, per-threshold metrics, calibration assessment hoàn tất.
- [ ] Monotonicity, A/B ablation, onset/continuation/unseen analysis có evidence.
- [ ] Frozen manifest trước 2024; post-test exploration được khai báo.
- [ ] Spark processing/MLlib và distributed/runtime evidence có lưu.
- [ ] Mỗi notebook action Spark đã có ảnh Spark Web UI sau trạng thái cuối: Jobs summary và từng Job detail; Application ID/Job ID, trạng thái, thời lượng, stages/tasks khớp evidence index, log và output.
- [ ] Notebook restart-and-run-all dùng cùng code/config và lưu outputs.
- [ ] Reload/inference thành công từ submission package trong fresh session.
- [ ] PDF có names/IDs/captions/limitations/sources/AI declaration chính xác.
- [ ] Package không credential/raw/cache/hard-coded personal paths.
- [ ] Cluster xóa, artifacts persist, remaining resources/costs đã kiểm tra.

| Ngày (SGT) | Output mục tiêu |
|---|---|
| 11/10 | Review; manifest/schema/time semantics; cloud preflight |
| 12/10 | Cloud smoke; quality Parquet/environment evidence; draft data/cleaning/environment |
| 13/10 | Task 1 training-year analysis; labels/features/leakage fixtures; draft Task 1/target/split |
| 14/10 | Three-model pilot, temporal selection/cost evidence |
| 15/10 | Full finalists; A/B ablation; calibration holdout fit |
| 16/10 | Freeze; final 2024 evaluation; Task 1 full-period results |
| 17/10 | LaTeX/README/package/model reload validation |
| 18/10 | Dự phòng lỗi/tái đóng gói; submit trước 23:59 |

Điều chỉnh timeline theo gates/runtime/credit. Ưu tiên data/target/split/availability đúng, hai tasks và ba models, evaluation/model reload/report. Neighbours, expanded rolling validation, runtime scaling và monotonicity correction chỉ sau phần chính; **calibration assessment, monotonicity assessment và A/B ablation vẫn thuộc phần chính**. Nếu giảm training cohort vì budget, ghi scope thực tế và đánh giá trên full held-out cohort khi khả thi, không claim full-data training.

**Acceptance:** final checklist có evidence; cluster/tài nguyên phụ được kiểm tra sau shutdown; package nộp đúng deadline và lưu receipt.

## Execution update — 11/10/2026

- Project thực dùng: `sg-rainfall-dsa5208-261011`, đã link đúng Education.
- 8 CSV + manifest đã upload; SHA256 custom metadata khớp. Cloud byte-level hashes do notebook 01 kiểm tra độc lập.
- Quota `SSD_TOTAL_GB=250`: cluster dùng master 100 GB + hai workers 75 GB balanced disk, thay request 300 GB bị API từ chối. Compute vẫn 3×e2-standard-4, image 2.2.87-debian12, private IP/IAP/Jupyter/event logging, 45 phút idle / 3 giờ maximum lifetime.
- `07_cloud_execution.ipynb` launch Jupyter kernels trên master và retrieve checkpointed notebooks; `08_cloud_diagnostics.ipynb` lưu read-only diagnostics; `09_refresh_source.ipynb` upload source bundle/hash trước new application. Chưa được đánh dấu EDA/model hoàn thành khi chưa có outputs thực.
- Spark giữ tối đa 10,000 jobs/stages/executions để tránh mất thông tin job trong các actions nhiều bước. Mọi Spark action vẫn phải review/capture UI thật.
- Notebook 04 chạy trong `application_1791706788393_0005` từ 10:15:53 UTC. Spark Jobs live UI lúc 11:15 UTC cho thấy 1,879 job records completed và Job 1879 active; thư mục model `gbt_A/` đã được ghi, nhưng `gbt_B/` và `models/frozen_manifest/` chưa có. Cluster đến hard-delete 11:17:32 UTC; lúc 11:17:58 trạng thái Dataproc là `DELETING` và SSH/IAP tới master đã mất kết nối. Không truy vấn được YARN final-state sau khi master dừng; do đó Notebook 04 được ghi là **incomplete**, không đánh dấu succeeded/failed khi chưa có final application record. Không chạy Notebook 05 hoặc đọc test metrics khi chưa có frozen manifest.
- Cluster ở trạng thái `DELETING` lúc 11:18:17 UTC và không còn xuất hiện trong project/region listing lúc 11:19:32 UTC; Dataproc active-job listing cũng trống. Do SSH/master đã mất trước khi truy vấn final YARN state, không có terminal YARN record để phân loại action thành success hay Spark failure; ghi nhận là incomplete do hard lifetime. Không gia hạn/tạo cluster mới khi người dùng chưa đăng nhập Console để đối chiếu số dư credit Education, pending costs và điều kiện tiếp tục. Credit ID/billing account đã dùng trong runtime vẫn là `3LU63BPGY77JFDQQ` / `01DAD8-AE5F1A-E3910A`; snapshot 50 USD ban đầu không phải số dư hiện tại.
- App5 Jobs summary và Job 1848 detail đã mở trực tiếp ở Spark Jobs UI; Job 1848 hiển thị `SUCCEEDED`, 1 giây, hai stage và 21/21 tasks mỗi stage. Đã lưu ảnh JPEG summary từ tab Spark UI còn giữ trong trình duyệt tại `report/evidence/spark-ui/20261011-full-01/app5-jobs-summary-active-checkpoint.jpg`. Ảnh được lưu lúc 14:13:22 UTC từ trang đã render trước đó, không phải lần tải mới; trang hiển thị 1,879 completed jobs và Job 1879 active sau 36 giây, 0/1 stage. Accessibility state lúc quan sát cho biết 2/8 tasks, nhưng cột tasks bị cắt khỏi viewport ảnh. Khi thử mở detail sau đó, local Spark UI proxy trả `ERR_CONNECTION_REFUSED`; ảnh này là checkpoint được giữ lại, không chứng minh trạng thái cuối/YARN. Job detail ảnh của app5 và ảnh summary/detail app4, app3, app1 vẫn thiếu; yêu cầu chụp summary và từng Job detail vẫn chưa đạt, Spark History API không thay thế ảnh.
- Editorial pass cho `report/main.tex`: đã thay Abstract và Conclusion bằng các phát hiện có số liệu của Task 1 cùng trạng thái dở dang chính xác của Task 2; các metric final-test chưa đo được ghi rõ là unavailable, không còn ô hướng dẫn biên tập hoặc `[Result]` giả. LaTeX biên dịch thành công. Báo cáo vẫn chưa đủ điều kiện nộp vì thiếu model freeze/test, cleaning reconciliation, 2024 post-freeze EDA và file ảnh Spark UI thật.
- Rà `fit_models()`/`threshold_metrics()` sau app5 cho thấy mỗi lần tính metric còn quét riêng để kiểm tra score không hợp lệ và khám phá threshold dù notebook đã biết grid. Đã gộp kiểm tra hợp lệ vào aggregate metric và truyền grid đã khai báo để bỏ bước khám phá dư thừa. Đây là tối ưu mã tĩnh, chưa chạy lại trên Spark nên chưa tuyên bố giảm runtime/job count; cần đo trong lượt chạy được cấp phép tiếp theo.
- Rà soát báo cáo EDA với các aggregate CSV đã xuất xác nhận lại các giá trị coverage, wet frequency, P99/exceedance, monthly extrema, hourly peak/range và common-cohort đều khớp sau rounding. Lưu ảnh checkpoint Spark Jobs app5 nêu trên; không mở được Job detail vì proxy đã từ chối kết nối sau khi cluster dừng. Không có compute mới được tạo.
- Để chuẩn bị retry an toàn sau khi qua billing gate, Notebook 04 dùng prefix `models/attempt-02/`, kiểm tra prefix chưa tồn tại và giữ nguyên thư mục `models/` dở dang của app5. Notebook 05 dùng cùng attempt ID, xác minh config/model root từ frozen manifest trước khi đọc test rows, đồng thời từ chối ghi đè thư mục final-evaluation đã có. Nếu attempt tiếp theo dở dang, phải giữ artifacts và chuyển cả hai notebook sang attempt ID mới; thay đổi này chưa được chạy trên cluster.
- Đính chính kiểm tra Console lúc 15:26 UTC ngày 11/10/2026: số USD 50.00/100% chỉ xuất hiện trong tab Credits cũ mở trước run, không được reload nên không xác nhận số dư sau run. Điều hướng/reload mới tới Credits, Reports và Cost table của đúng Billing Account for Education `01DAD8-AE5F1A-E3910A` đều cho trang trống; số dư hiện tại, cost line items và pending usage chưa xác minh. Không phát sinh compute mới; giữ cloud gate đóng cho tới khi có thể đọc các thông tin này cùng chi phí storage/retention.
- Recheck sau đó bằng gcloud chỉ đọc: project/region không còn Dataproc cluster, Compute VM, persistent disk hoặc reserved address; bucket `gs://sg-rainfall-dsa5208-261011-data` ở `ASIA-SOUTHEAST1` còn 11.43 GiB. Đây là kích thước lưu trữ, không phải số tiền đã tính; balance, cost table, khoản pending và storage charges chưa xác minh. Trong lần mở/reload Console kế tiếp, trang Credits vẫn trắng.
- Đã sửa gate cho các lần chạy sau: `src/rainfall/billing.py` kiểm tra đúng Education account + credit ID, danh sách credit đủ điều kiện, ba trang Credits/Reports/Cost table, timestamp còn mới, usage/pending, giới hạn gross USD 40 và reserve USD 10; session được ước tính tối đa USD 8. Notebook 00 mặc định `CREDIT_LIVE_VERIFIED=False`; runtime manifests cũ không có snapshot mới và các notebook cloud sẽ fail closed. Chưa bật lại gate hoặc tạo compute.
- `notebooks/11_spark_ui_evidence.ipynb` được sửa để `index.csv` có một dòng cho mỗi notebook action, liệt kê Job IDs/trạng thái/thời lượng/stages/tasks và để screenshot ở trạng thái pending đến khi chụp trang Spark Web UI thật. API metadata được lưu riêng; thay đổi này chưa chạy lại và không bổ sung ảnh screenshot còn thiếu.
- Recheck ngày 12/10/2026: Chrome hiển thị Credits page của Billing Account for Education với credit `3LU63BPGY77JFDQQ` còn 96% / USD 47.91 trên USD 50.00. Đây là mức giảm USD 2.09 ở cấp account, chưa đủ chứng minh project này tiêu đúng khoản nào. Reports/Cost table/pending usage chưa được xác minh nên tiếp tục giữ cloud gate đóng; không tạo cluster hay Spark job mới.

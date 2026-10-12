# DSA5208 Project 2 — trạng thái hiện hành

Cập nhật ngày **12/10/2026**. Bài cá nhân: **DAM MINH TIEN — A0355091E**.
Đây là checklist thực tế cho bốn notebook hiện hành; `TASK_SPEC.md` giữ lại
đặc tả và nhật ký cũ để tham khảo. Không coi thiết kế dự kiến là kết quả đã chạy.

## Run đã hoàn thành

- **Run ID:** `20261012-full-02`
- **Code commit dùng cho run:** `ea454b3eb1abdb2059dfba283091ca3b80f3c3fe`
- **GCS root:** `gs://sg-rainfall-dsa5208-261011-data/runs/20261012-full-02/`
- **Config SHA-256:** `daeebb8814a3157e3347ecceb152b7e48c47310df1071b7dca08d9622c0d62d6`
- **Manifest SHA-256:** `41080cf58fc68f899c06145d573620b61777bbcc3b13d90c69020a6dcd790688`

### Đã chạy và có evidence

- [x] Notebook 01: 34 code cells đã thực thi, 0 lỗi, 10 figures inline.
- [x] Notebook 02: 12 code cells đã thực thi, 0 lỗi.
- [x] Notebook 03: 41 code cells đã thực thi, 0 lỗi, 1 figure inline.
- [x] Cleaning đủ 2017–2024: 48,538,713 raw rows; giữ 48,538,627; loại 86.
- [x] Task 1 đủ tám năm. Wet-reading frequency theo năm từ 1.94% (2019) đến
  3.20% (2023); common annual station cohort rỗng ở cả ngưỡng coverage 90%,
  95% và 99%, nên không kết luận annual trend trên fixed network.
- [x] Đã xem phân phối timestamp trước cleaning. Năm 2017 có 1,775,327 dòng
  offset 299 giây; run dịch **epoch phân tích** thêm 1 giây cho block đó và
  giữ nguyên timestamp nguồn. Đây là giả định chưa được publisher xác nhận,
  cần nêu trong report.
- [x] Task 2: train 2017–2022; model selection Jan–Jun 2023; calibration Jul–Dec
  2023; holdout 2024. Đã chọn `gbt_B` trên selection, hiệu chuẩn isotonic,
  rồi chỉ so sánh model được chọn với climatology trên holdout.
- [x] Ở ngưỡng 0.5 mm trên 6,374,476 valid prediction times, calibrated
  `gbt_B` đạt PR-AUC 0.6345 và Brier 0.01952; climatology đạt 0.0732 và
  0.03436. F1 tại 0.5 chỉ để tham khảo; cutoff không được optimize.
- [x] Hình do code notebook tạo và bảng/provenance của `full-02` được lưu ở
  `report/figures/full-20261012/` và `report/tables/full-20261012/`.
- [x] Model, calibrator, climatology baseline và selected manifest đã tải về
  `artifacts/models/20261012-full-02/` (khoảng 2.6 MB).
- [x] Fresh Spark 3.5.3 session đã load pipeline GBT 5 stages, isotonic
  calibrator, ba climatology tables và selected manifest; một prediction mẫu
  0.5 mm chạy thành công. Raw p=0.02881781, calibrated p=0.02589584.
  Đây chỉ là serialization/inference smoke check, không chạy lại holdout.
  Dataproc job `ee582912264541edb80942445a23994f` đã `DONE`; cluster một node
  `rainfall-reload-20261012` đã bị xóa. Evidence ở
  `report/tables/full-20261012/model_reload_validation.json`.
- [x] Report đã được cập nhật từ số liệu run này và dùng đúng tên project
  **Singapore Rainfall Analysis and Prediction** trên trang bìa/metadata; xem
  `report/main.tex`.
- [x] PDF biên dịch thành công bằng `latexmk` và đã rà lại bố cục/bảng/hình.
- [x] ZIP nộp gồm source, notebooks, config/manifest, PDF, bảng/hình dùng trong
  report và selected model artifacts; không có raw CSV hay workstation files.
  SHA-256: `7d44f379baa369243868c572790f47b16f61e3687fc92b1b35991c9017103bb4`.

## Còn lại trước khi nộp

- [ ] **Notebook 00 evidence:** setup cloud đã chạy trong workflow `full-02`,
  nhưng local `notebooks/00_setup_cloud.ipynb` không giữ execution outputs.
  Vì vậy không đánh dấu setup notebook là restart-and-run-all reproducible.
- [ ] **Nộp Canvas:** kiểm tra định dạng/tên file theo trang bài tập, upload
  gói cuối và xác nhận receipt. Repo không có bằng chứng upload.
- [ ] Nếu cần chạy lại cloud, xác nhận project/quota/billing hiện hành và dùng
  run prefix mới; không tái sử dụng `20261012-full-02` như một run mới. Chỉ
  dùng nguồn credit đã được người dùng chỉ định; không dùng paid fallback.

## Thiết kế hiện hành

| Notebook | Vai trò | Tình trạng run `full-02` |
|---|---|---|
| `00_setup_cloud.ipynb` | Chuẩn bị dữ liệu/config và Dataproc | Đã dùng; output local không lưu |
| `01_clean_and_task1.ipynb` | Cleaning và Task 1, đủ 2017–2024 | Hoàn tất, outputs đã lưu |
| `02_build_features.ipynb` | History features, target, splits | Hoàn tất, outputs đã lưu |
| `03_train_evaluate.ipynb` | Tuning, selection, calibration, 2024 test | Hoàn tất, outputs đã lưu |

Core science phải đọc được ngay trong notebooks. Không đưa billing/runtime
assurance, source-bundle/version ledger hay screenshot-per-job workflow vào
điều kiện chấm bài. Model MLlib được fit trên deterministic 2% sample của
valid training times; climatology dùng toàn bộ valid training partition.
Jan–Jun 2023 là tập chọn mô hình, không phải một ước lượng generalization
độc lập. Giai đoạn này được dùng cả để tuning pilot lẫn so sánh candidate
cuối; score của model được chọn vì thế có xu hướng lạc quan và không phải
unbiased validation score. Holdout 2024 không được dùng để tune sau test.

## Những giới hạn cần giữ trong report

- Timestamp được hiểu theo interval-end convention như một giả định; định nghĩa
  timestamp chưa được NEA xác nhận. Riêng block offset 299 năm 2017 được dịch
  epoch +1 giây như mô tả ở trên.
- Dữ liệu lưu trữ là retrospective. Không tái dựng thời điểm các revision lần
  đầu xuất hiện; không tuyên bố hiệu năng real-time.
- Missing current observation nằm ngoài prediction contract. Dự đoán chỉ tại
  valid observation timestamps; label cần đủ sáu future readings đúng grid.
- Annual descriptive summaries dùng network thay đổi; common-station cohort
  rỗng. Không gọi khác biệt giữa năm là climate trend.
- Isotonic GBT ECE trên 2024 là 0.00172, hơi cao hơn climatology 0.00142.
  Calibration trigger/fit không chứng minh model hiệu chuẩn tốt hơn baseline.
- Monotonicity violation rate của calibrated GBT là 0.0121% ở sáu primary
  thresholds và 0.0151% ở năm intermediate thresholds.

## Kiểm tra hồ sơ trước khi nộp

- [x] Đọc PDF sau khi compile, rà trang bìa, bảng/hình, references và số liệu.
- [x] Kiểm tra ZIP khớp với `20261012-full-02` và không chứa raw CSV hoặc file
  workstation. Package path:
  `submission/DSA5208_Project2_A0355091E_full-20261012-full-02.zip`.
- [x] README/report ghi rõ fresh reload smoke check đã pass và không tuyên bố
  đã chạy lại holdout.

# Project Memory — DSA5208 Singapore Rainfall

Cập nhật: 12/10/2026 (Asia/Singapore). Đây là ghi chú để khôi phục bối cảnh khi làm việc tiếp; các trạng thái cloud/cost bên dưới là snapshot có ngày, phải kiểm tra lại trước khi chạy hoặc kết luận hiện trạng.

**Quy tắc tiếp tục:** trước khi thực hiện yêu cầu tiếp theo trong project, đọc lại file này cùng `data/TASK.md`. Dùng các quyết định đã chốt bên dưới làm bối cảnh mặc định, không hỏi lại những câu đã được trả lời; xác minh lại mọi trạng thái cloud thay đổi theo thời gian. Cập nhật phần trạng thái khi có thêm bằng chứng hoặc quyết định mới.

## Mục tiêu và phạm vi

- Project cá nhân DSA5208 Project 2: Singapore Rainfall Analysis and Prediction.
- Tác giả duy nhất: **DAM MINH TIEN**, Student ID **A0355091E**.
- Bám đúng hai tasks trong đề: Task 1 phân tích rainfall patterns trong năm/giữa năm; Task 2 dự đoán xác suất lượng mưa tích lũy tại một station trong 30 phút tới **lớn hơn** threshold.
- Không tự đặt research question, novelty claim hoặc rubric weight. Project 1 chỉ làm tài liệu tham khảo hình thức.
- Báo cáo là phần được đánh giá nặng: ưu tiên viết có evidence, số liệu thật, mẫu số/cohort/giới hạn rõ. Abstract viết cuối sau khi có kết quả cuối.
- Deadline trong mô tả đề: 18/10/2026 23:59; timezone và giới hạn nộp cần đối chiếu Canvas.

## Quyết định đã chốt

- Người dùng chọn quy ước `timestamp` là **cuối khoảng đo 5 phút**. Đây là giả định được chấp nhận cho bài, không phải xác nhận của NEA. Phân loại kết quả là retrospective event-time prediction khi semantics/availability của update fields chưa được xác minh; không tuyên bố real-time.
- Dataset đúng tám annual CSV, 2017–2024; không lấy 2016 làm warm-up. Raw giữ bất biến ngoài repo. Manifest ở `manifests/input_manifest.json`; retrieval time gốc chưa biết.
- Project code phải được gọi/chạy qua Jupyter Notebook để output hiện trong notebook. Setup notebook 00 chạy trên Mac được; Spark processing/model training chạy trên Dataproc Jupyter, không dựng local Spark cluster. Reusable functions ở `src/`/`scripts/` được import từ notebook.
- Mỗi Spark action: mở Spark Jobs UI/History Server sau terminal state, lưu Jobs summary screenshot và từng Job detail screenshot thật. Dataproc Jobs/YARN/API metadata không thay screenshot. Action không sinh job phải ghi no-jobs.
- Cloud chỉ dùng project `sg-rainfall-dsa5208-261011`, region `asia-southeast1`, Billing Account for Education `01DAD8-AE5F1A-E3910A`, credit ID `3LU63BPGY77JFDQQ`. Không đổi account, dùng credit khác, thêm payment method hoặc paid fallback.
- Người dùng yêu cầu tiếp tục mà không chờ Reports/Cost table hoặc hỏi lại số cost. Credit tự động được áp dụng ở billing-account scope; gcloud không pin Credit ID vào từng resource/job. Gate code phải xác minh project/account linkage và credit ID đã được người dùng cho phép, không bắt điền cost/pending giả.

## Trạng thái cloud đã kiểm tra ngày 12/10/2026

- `gcloud` active identity là `damminhtienchl@gmail.com`.
- Project `sg-rainfall-dsa5208-261011` ở trạng thái ACTIVE, `billingEnabled=true`, account name `billingAccounts/01DAD8-AE5F1A-E3910A`.
- Danh sách billing accounts có thêm `My Billing Account`, nhưng project này hiện link đúng Education; tuyệt đối không link account kia.
- Dataproc cluster list trong `asia-southeast1` rỗng tại lần kiểm tra.
- Bucket `gs://sg-rainfall-dsa5208-261011-data`: location `ASIA-SOUTHEAST1`, STANDARD, uniform bucket access, khoảng **11.43 GiB** lúc kiểm tra; xóa cluster không xóa GCS storage.
- Browser Console đang hiện đúng Education account và credit `3LU63BPGY77JFDQQ`: **96%, USD 47.91 trên USD 50**. Đây là số dư credit account-level tại lúc chụp, không phải khoản cost đã gán cho project.
- Không có Dataproc cluster hiện hoạt trong snapshot trên. Trước mỗi thao tác cloud tiếp theo, đọc lại billing link, resource inventory và bucket prefixes.

## Dữ liệu và protocol chính

- Manifest inventory: 48,538,713 rows, 8,047,044,701 bytes; tám file có cùng schema 12 fields và timestamps `+08:00`; retrieval time gốc không có.
- Timestamp alignment exact 300 seconds hiện cấu hình `alignment_tolerance_seconds=0`. Bản ghi 2017 bắt đầu `08:04:59`; không được floor/round tự động sang grid.
- Cleaning policy dùng raw provenance, explicit schema, rainfall finite/nonnegative, unit mm/type đúng, exact duplicate tách riêng, conflicting `(station_id, epoch)` quarantine; missing không đổi thành zero; raw = retained + disjoint excluded reasons phải reconciliation đúng theo năm.
- Target: `R30(s,t)=sum(r(s,t+5k min), k=1..6)` và `Y=1[R30>threshold_mm]`; interval-end convention; đúng bằng threshold là negative; cần đủ 6 future intervals contiguous, không nhảy gap.
- Temporal split: train 2017–2022; selection Jan–Jun 2023; calibration Jul–Dec 2023; final test 2024; purge label endpoint qua split. 2024 label-valid counts đã được xem trước freeze, nhưng theo log cũ chưa xem prevalence/model scores; không được mô tả là hoàn toàn blind.
- `config/experiment.json`: train thresholds `[0.2,0.5,1,2,5,10]` mm; unseen `[0.3,0.75,1.5,3.5,7.5]`; domain `[0.2,10]`; lag 5/10/15/30/60 min; rolling 15/30/60/180 min.
- Candidate training hiện cấu hình pilot fraction 0.5%, final ML train fraction 2%, seed 5208; LR regParam `{0.001,0.01,0.1,1}`, maxIter 50; GBT `(depth,iter)` `{(3,20),(4,20),(3,40),(4,40)}`, maxBins64. Baseline climatology dùng toàn eligible train, nên khác sample scope với LR/GBT.
- Calibration: selection ECE trigger 0.01; nếu bật, pooled isotonic fit trên calibration holdout; 10 reliability bins; probability cutoff grid `{0.05,0.1,0.2,0.3,0.5}`; 1000 paired calendar-week bootstrap replicates, seed5208.
- Spark current config: image `2.2.87-debian12`; master+2 workers `e2-standard-4`; 100GB master /75GB worker balanced disks; driver/executor request 4g; 4 executors ×2 cores, dynamic allocation off; 64 shuffle partitions; UI retains10,000 jobs/stages/SQL executions. Prior listener OOM/hard-delete is reason to checkpoint and measure, not evidence current runtime completed.

## Evidence hiện có và công việc chưa xong

- `report/tables/pretest/` has Task 1 aggregate CSVs and `findings.json` for 2017–2023 only. Findings: no annual common station cohort even at 90% coverage; full eight-year controlled annual comparison is unavailable.
- Previous feature notebook `notebooks/executed/03_task2_features-attempt-1.ipynb` completed target fixtures and materialized base features; its outputs are historical run evidence, not proof current resources/artifacts are present.
- Previous cleaning output `cleaned/` had `_SUCCESS`, but the cleaning action timed out/listener OOM before rejected/exclusion/alignment/quality/coverage outputs and reconciliation were produced. Do not call cleaning complete just because `_SUCCESS` exists.
- Previous model run application `application_1791706788393_0005` ended at Dataproc 3-hour hard delete while Job 1879 was still active; `gbt_A` exists but `gbt_B` and frozen manifest were absent in the record. No final model freeze or 2024 final metrics has been verified.
- Previous screenshots: an app5 Jobs-summary active checkpoint exists, but app5 job detail and app1/app3/app4 full summary/details were missing from evidence files. Screenshot requirement is still open.
- `report/main.tex` compiled in an earlier turn, but it is not final without cleaning reconciliation, model freeze/test results, eight-year EDA, complete Spark UI screenshots, package reload, and submission receipt.
- Prior notebook run `RUN_ID=20261011-full-01` has many artifacts in the GCS bucket. Keep it immutable; inspect each output/metadata before reuse. A fresh retry must use a distinct run ID and output prefix.

## Điểm dừng lần tiếp tục gần nhất — 12/10/2026

- `data/TASK.md` là checklist hiện hành. Mục A (đồng bộ billing policy và run-scoped paths) đã được hoàn tất, commit `9392908` (`Fix cloud billing gate and version run artifacts`). Commit riêng trước đó `c351f97` thêm ghi chú bộ nhớ ban đầu.
- Run dự kiến tiếp theo là `RUN_ID=20261012-full-01`, `MODEL_ATTEMPT=attempt-01`; prefix runtime/source bundle mới được kiểm tra rỗng ở lần xác minh gần nhất. Giữ run `20261011-full-01` bất biến.
- `notebooks/00_environment.ipynb` và analysis notebooks 01–05 đã được cập nhật để dùng manifest/source bundle theo run mới; notebook hỗ trợ 07–11 cũng đã đồng bộ. Static checks đã qua: `py_compile` cho billing helper, JSON/AST checks cho notebook code cells và rà soát old cost gates.
- Chưa chạy notebook00, chưa tạo cluster cho run mới, và chưa có kết quả mới cho B–F. Không coi các static checks này là bằng chứng pipeline đã chạy.
- Bước triển khai kế tiếp: xác nhận đúng cửa sổ/tab `00_environment.ipynb` trong VS Code, chọn Python 3.14 local kernel cho setup notebook, chạy cell theo thứ tự và xem output từng bước. Trước cell tạo tài nguyên, kiểm tra lại live identity, project/account link và inventory; Spark workload chỉ chạy trên Dataproc Jupyter.
- VS Code UI ở lần cuối còn hiển thị lẫn tab notebook cũ/mới; phải xác nhận lại tên tab và kernel bằng UI trước khi chạy để tránh chạy nhầm notebook.
- Git có thay đổi có sẵn ngoài phạm vi ghi chú này (`README.md`, `report/main.tex`, ba module `src/rainfall`, `.DS_Store`, `report/evidence/`, `report/figures/`, `report/tables/`). Không stage/revert các mục đó khi cập nhật memory.

## Working conventions

- The current task checklist is `data/TASK.md`; detailed original protocol/history is in `data/TASK_SPEC.md`.
- Read project `AGENTS.md` instructions before changes. Prefer focused edits, preserve pre-existing dirty/untracked user artifacts, and commit only explicit relevant paths after inspecting diff and running `git diff --check`.
- Do not present planned parameters as selected model settings or partial traces as completed results. Verify current state from files/notebook outputs/Cloud APIs before relying on this memory.

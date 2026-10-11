# Khung báo cáo Project 2 — bài cá nhân

Source: `main.tex`. Đây là khung draft, chưa phải báo cáo kết quả. Nội dung tiếng Anh; hướng dẫn điền bên dưới bằng tiếng Việt. Áp dụng skill `data/academic-writing-SKILL.md` và yêu cầu chính thức trong `data/project_description.txt`.

Trang bìa chỉ có **một tác giả và một student ID**. Điền `\studentname` và `\studentid` trong LaTeX. Bố cục A4 một cột được tái dựng theo hình thức PDF Project 1; chưa có source template gốc.

## Sườn và nội dung cần hoàn thiện

| Mục | Viết gì | Bằng chứng/output cần có | Gợi ý trang |
|---|---|---|---|
| Abstract | Hai tasks, phương pháp thực chạy, findings chính, giới hạn | Số liệu đã khóa từ Task 1/2; viết cuối | 150–200 từ |
| 1. Introduction | Bài toán và phạm vi thực hiện | Đề bài; không cần research questions riêng | 0.5 |
| 2. Dataset and Preprocessing | Nguồn, timestamps/units, cleaning, coverage, attrition | Audit theo năm; rules xử lý duplicates/gaps/missing | 1–1.5 |
| 3. Computing Environment | Cấu hình thực tế và cách chạy Spark/MLlib | Runtime/VM config, worker evidence, stage timings | 0.5 |
| **4. Task 1: Rainfall Patterns** | Coverage/denominators; dry/wet distribution; monthly/hourly; common-cohort annual changes; findings | Coverage heatmap; distribution; monthly/hourly with support; annual comparison at 90/95/99%; supplementary complete-day/tail/spell/spatial panels | **3–4** |
| **5. Task 2: Rainfall Probability Prediction** | Target/input; features; splits; models; performance; calibration; errors | Feature/split tables; baseline/LR/GBT comparison; threshold metrics; PR/reliability plots; A/B summary | **3–4** |
| 6. Discussion and Limitations | Kết nối patterns và model; giải thích giới hạn thật | Findings đã trình bày; không thêm claim không có evidence | 0.5–1 |
| 7. Reproducibility and Conclusion | Chạy lại/load model; kết luận hai tasks | Package/README và checks thực sự đã làm | 0.5 |
| AI declaration / References / Appendix | Khai báo cá nhân; nguồn thật; chi tiết bổ sung | Tool/model/uses thực tế; references đã dùng | Ngoài nội dung chính |

Đây là phân bổ biên tập, không phải page limit hoặc trọng số điểm của thầy. Task 1/2 phải chiếm phần lớn nội dung. Không bắt buộc đủ số hình/bảng trong khung nếu một artifact không giúp giải thích kết quả.

## Cách viết đoạn kết quả

1. Nêu phát hiện có số liệu và đơn vị.
2. Cho biết tập dữ liệu/cohort và cách tính đủ để hiểu so sánh.
3. Giải thích phát hiện có ý nghĩa gì đối với task.
4. Nêu giới hạn cụ thể nếu ảnh hưởng đến cách hiểu.

Không chỉ mô tả màu biểu đồ hoặc đọc lại từng ô bảng. Không biến giả thuyết nguyên nhân thành kết luận đo được. Nếu ML không vượt baseline, trình bày đúng. Một kết quả ở abstract, bảng và conclusion phải cùng cohort, số liệu và rounding.

## Thứ tự viết

- Viết data/cleaning/environment và target/features/split khi các quyết định đã xác nhận.
- Viết Task 1 sau khi có aggregates; viết Task 2 sau khi có frozen evaluation.
- Hoàn thiện discussion/conclusion, rồi abstract.
- Thay mọi `Draft instruction`, `[Result]`, `[Count]` và figure placeholders bằng nội dung thật. Các hướng dẫn draft được cố ý hiển thị để dễ rà soát; không dùng cơ chế ẩn để che các mục chưa hoàn thành.
- Figure captions có units, thời gian/split, cohort, statistic và missingness rule; calibration có bin counts. Export aggregates nhỏ từ Spark, dùng SciencePlots cho hình cuối.
- Compile và đọc PDF; đối chiếu số liệu/source outputs, bảng/hình/cross-references và yêu cầu đề trước khi nộp.

## Mục còn cần thông tin hoặc kết quả

- Tác giả đã xác nhận: DAM MINH TIEN, A0355091E.
- Source LaTeX Project 1 nếu muốn dùng chính template gốc.
- Interval-end convention đã được chấp nhận như giả định; cần giữ rõ distinction với xác nhận NEA. Cleaning audit và actual station coverage vẫn cần run evidence.
- Actual cloud runtime sau preflight đúng credit/billing được chỉ định.
- Task 1 findings, trained pipelines, final metrics và model reload evidence.
- Tool/model và toàn bộ công việc có AI hỗ trợ để hoàn thiện declaration.

Khung LaTeX có thể mở trực tiếp trong Codex để xem PDF preview. Khi dùng compiler ngoài, chạy từ `report/` với một LaTeX distribution có các packages trong preamble; compile hai lượt để cập nhật mục lục. PDF/auxiliary build files không đưa vào commit source.

## EDA bổ sung

Phần methods trong `main.tex` đã xác định wet/dry, coverage, equal-station và pooled weighting, hourly eligibility, complete-day filtering, train-only P99 reference, common-cohort sensitivity, stable station coordinates và gap-censored wet spells. Không cần dựng câu hỏi nghiên cứu mới.

Notebook 02 xuất EDA 2017–2023, notebook 05 xuất đủ 2017–2024 sau model freeze. Dùng `findings.json` và CSV để viết peak/trough với mm hoặc percentage points và support; chưa có run outputs thì giữ trạng thái pending. Khối `edaplot` sẽ dùng actual PDF ở `report/figures/` khi file có mặt; trước đó hiển thị rõ đang chờ measured output. Chưa có figure thì không thay bằng chart minh họa.

Giữ thân Task 1 khoảng 3–4 trang sau khi rút gọn methods và thay notes bằng actual findings. Đưa full quantile, complete-day, yearly-hourly, station và wet-spell evidence sang appendix khi có phân tích liên quan. Đây là phân bổ biên tập, không phải yêu cầu page limit của thầy.

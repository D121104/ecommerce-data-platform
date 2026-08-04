# Báo cáo tiến độ hiện tại — E-commerce Data Platform

**Ngày đánh giá:** 28/07/2026  
**Căn cứ đánh giá:** đối chiếu hiện trạng repository với [kế hoạch dự án](ke-hoach-ecommerce-data-platform.md).  
**Phạm vi đánh giá:** mã nguồn, cấu hình, Docker Compose, SQL khởi tạo và kiểm thử hiện có trong repository. Không có bằng chứng về một lần chạy end-to-end thành công tại thời điểm lập báo cáo.

## 1. Tóm tắt điều hành

Dự án đang ở giai đoạn **hoàn thiện nền tảng ban đầu và triển khai một phần ingestion từ Platzi API**. Repository đã có cấu trúc khởi tạo, container PostgreSQL, phân quyền/schema, các bảng `raw`/`ops`, API client có pagination cho products, retry HTTP, Pydantic validation và một số unit test.

Các thành phần cốt lõi để hoàn thành MVP vẫn chưa được triển khai: loader PostgreSQL, raw response theo trang, order generator quyết định/idempotent, dbt project/models/snapshots/tests, Airflow DAG, Metabase dashboard, CI và tài liệu vận hành đầy đủ.

**Đánh giá tiến độ tổng thể (ước lượng theo phạm vi MVP): khoảng 20%.** Con số này phản ánh mức độ hiện diện của các deliverable trong repository, không phải tỷ lệ thời gian đã sử dụng.

## 2. Tình trạng theo giai đoạn kế hoạch

| Giai đoạn | Nội dung kế hoạch | Trạng thái | Đánh giá hiện tại |
|---:|---|---|---|
| 0 | Chuẩn bị môi trường và repository | Đang thực hiện | Đã có khung thư mục, packaging Python, tệp môi trường mẫu và Docker Compose cho PostgreSQL. `Dockerfile` và `Makefile` hiện rỗng; dependencies chưa được cài trong môi trường đánh giá. |
| 1 | PostgreSQL và raw schemas | Hoàn thành một phần | Đã có script tạo role, schema `raw`, `ops`, `staging`, `snapshots`, `warehouse`, `marts`; đã có bảng `ops.pipeline_runs`, `ops.rejected_records`, `raw.categories`, `raw.products`, `raw.users`, `raw.orders`, `raw.order_items`. Chưa xác minh migration/container chạy thành công. |
| 2 | Platzi API ingestion | Hoàn thành một phần | Đã có API client GET cho `categories`, `products`, `users`; products hỗ trợ `offset/limit`, giới hạn trang, phát hiện trang lặp, timeout và retry cho lỗi transport/429/5xx. Đã có Pydantic validation và loại bỏ trường `password`. Chưa có orchestration extraction, lưu raw response, loader/upsert PostgreSQL hoặc xử lý rejected records vào database. |
| 3 | Order generator | Chưa bắt đầu | Chưa có module sinh đơn hàng, seed theo logical date, phân bố trạng thái, tính `line_amount` hoặc cơ chế idempotency cho dữ liệu đơn hàng. |
| 4 | dbt staging | Chưa bắt đầu | Cây thư mục dbt đã tạo nhưng chỉ chứa placeholder. Chưa có `dbt_project.yml`, profile, source, staging models hoặc dbt tests. |
| 5 | Warehouse và SCD2 | Chưa bắt đầu | Chưa có snapshots, dimension/fact tables, incremental models, SCD Type 2 hoặc marts. |
| 6 | Airflow | Chưa bắt đầu | Thư mục DAG chỉ có placeholder. Docker Compose chưa có Airflow; DAG `ecommerce_daily_pipeline` chưa tồn tại. |
| 7 | Metabase | Chưa bắt đầu | Thư mục dashboards chỉ có placeholder. Docker Compose chưa có Metabase; chưa có dashboard hoặc truy vấn marts. |
| 8 | Tests và CI | Hoàn thành một phần | Có test pagination, retry 503 và loại bỏ password. Chưa có test generator, validation lỗi, loader/idempotency, integration test, dbt test hoặc GitHub Actions. Lệnh `pytest` hiện dừng ở bước collection vì thiếu package `httpx`. |
| 9 | README và demo | Chưa bắt đầu | README chỉ mô tả ngắn và ghi trạng thái “Project initialization”; chưa có hướng dẫn chạy, kiến trúc chi tiết, data model, demo, ảnh hoặc video. |

## 3. Hạng mục đã triển khai

### 3.1. Nền tảng repository và cấu hình

- Đã có cấu trúc thư mục tương ứng với ingestion, SQL init, dbt, Airflow, dashboard, test và docs.
- Đã khai báo project Python từ `src/` với Python >= 3.12.
- Đã khai báo dependency cho HTTP client, validation, retry và test/lint.
- Đã có `.env.example` tách thông tin PostgreSQL, các role database và cấu hình Platzi API.
- Đã có PostgreSQL service trong Docker Compose, volume bền vững, healthcheck, timezone UTC và network riêng.

### 3.2. PostgreSQL raw và observability nền tảng

- Đã tách quyền theo ingestion, dbt và BI reader.
- Đã tạo các schema đích theo thiết kế: `raw`, `ops`, `staging`, `snapshots`, `warehouse`, `marts`.
- Đã có bảng theo dõi lần chạy pipeline và bảng rejected records.
- Đã có bảng raw cho catalog, users, orders và order items; có check constraint, foreign key và index cơ bản.
- Đã không lưu password trong `raw.users` thông qua ràng buộc payload.
- Đã giữ `unit_price` trên order item, phù hợp yêu cầu lưu giá tại thời điểm mua.

### 3.3. Khai thác và kiểm tra dữ liệu API

- API client chỉ sử dụng GET.
- Products được phân trang với `offset/limit`; dừng khi trang rỗng hoặc thiếu số bản ghi của một trang đầy đủ.
- Có giới hạn số trang và phát hiện trang lặp để tránh vòng lặp vô hạn.
- Retry áp dụng cho lỗi transport, HTTP 429 và HTTP >= 500; lỗi 4xx khác không retry.
- JSON bắt buộc là list các object; lỗi cấu trúc được ném ra rõ ràng.
- Pydantic models kiểm tra ID dương, tên bắt buộc, giá không âm và email hợp lệ.
- Payload được làm sạch đệ quy để không giữ trường `password`.
- CLI đã có thể gọi extraction/validation cho từng entity hoặc tất cả entity, nhưng mới in kết quả ra terminal.

## 4. Chênh lệch quan trọng so với kế hoạch

### 4.1. Raw layer chưa đáp ứng đầy đủ thiết kế batch snapshot

Thiết kế kế hoạch yêu cầu lưu nguyên response theo từng trang tại `raw.api_responses` và lưu snapshot theo cặp `(source_id, batch_id)`. Hiện chưa có `raw.api_responses`; các bảng catalog dùng `id` nguồn làm khóa chính nên chỉ biểu diễn trạng thái hiện tại của mỗi entity, không thể lưu nhiều snapshot của cùng entity giữa các batch như thiết kế ban đầu.

Cần quyết định và thống nhất một trong hai hướng trước khi viết loader/dbt:

1. Giữ raw layer dạng snapshot theo batch như kế hoạch, đổi khóa thành khóa ghép hoặc thêm surrogate key; hoặc
2. Giữ raw layer dạng current-state, đồng thời bổ sung bảng response/snapshot lịch sử riêng cho change detection và dbt snapshot.

### 4.2. Chuẩn trạng thái đơn hàng chưa nhất quán

Kế hoạch warehouse yêu cầu các trạng thái `pending`, `completed`, `cancelled`, `refunded`, trong khi bảng `raw.orders` hiện dùng `pending`, `paid`, `shipped`, `delivered`, `cancelled`. Khi triển khai generator và dbt cần chuẩn hóa mapping rõ ràng để dashboard và dbt tests không sai lệch.

### 4.3. Không thể xác minh test tại thời điểm đánh giá

Đã chạy `pytest`, nhưng test bị dừng trong quá trình import do môi trường chưa cài `httpx`. Do đó các test hiện có chưa được xác nhận pass. Cần cài dependencies bằng môi trường Python quản lý phù hợp trước khi coi giai đoạn nền tảng/test là hoàn tất.

## 5. Công việc ưu tiên tiếp theo

1. **Hoàn tất khả năng chạy môi trường:** bổ sung nội dung cho Dockerfile/Makefile nếu cần, tạo môi trường Python, cài dependencies và chạy lại `pytest` cùng lint.
2. **Chốt thiết kế raw theo batch:** bổ sung `raw.api_responses`, xác định chiến lược snapshot/current-state và điều chỉnh schema trước khi có dữ liệu thật.
3. **Triển khai ingestion end-to-end:** tạo batch run, extract từng entity, validate, ghi rejected records, tính hash, nạp/upsert PostgreSQL và ghi metrics/log an toàn.
4. **Triển khai order generator:** seed theo logical date, UUID/order ID quyết định, phân bố đơn hàng, order items, discount/line amount và kiểm tra chạy lại không trùng.
5. **Dựng dbt:** tạo project/profile, sources, staging, snapshot sản phẩm, warehouse dimensions/facts, marts và tests.
6. **Dựng Airflow:** thêm services và DAG chạy đúng luồng daily pipeline, cấu hình retry, `max_active_runs=1`, `catchup=False`.
7. **Hoàn thiện chất lượng và demo:** integration tests, CI GitHub Actions, Metabase dashboards, README vận hành và tài liệu kiến trúc/data model.

## 6. Tiêu chí chuyển sang giai đoạn tiếp theo

Có thể coi Giai đoạn 1–2 hoàn tất khi đạt đồng thời các điều kiện sau:

- PostgreSQL khởi động thành công bằng Docker Compose và các script init chạy không lỗi.
- Dependencies Python được cài và toàn bộ unit test hiện có chạy xanh.
- Ingestion lấy đủ `products`, `categories`, `users` từ API, bao gồm pagination cho products.
- Mỗi batch có record trong `ops.pipeline_runs`; raw response, record hợp lệ và rejected record đều được lưu đúng nơi.
- Chạy lại ingestion không tạo dữ liệu không kiểm soát và metrics pipeline được cập nhật.

## 7. Kết luận

Nền móng kỹ thuật đã được đặt đúng hướng, đặc biệt ở phân tách quyền PostgreSQL, validation dữ liệu đầu vào, an toàn đối với password và phòng vệ pagination/retry. Tuy nhiên, dự án hiện chưa có luồng dữ liệu hoàn chỉnh từ API đến database, và toàn bộ phần warehouse, điều phối, BI, CI vẫn còn phía trước. Trọng tâm thực hiện tiếp theo là hoàn thành ingestion PostgreSQL có batch/idempotency, sau đó mới chuyển sang order generator và dbt.

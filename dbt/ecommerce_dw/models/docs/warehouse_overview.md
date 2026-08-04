{% docs warehouse_overview %}

# Ecommerce Data Warehouse

Data warehouse chuyển dữ liệu thương mại điện tử từ source PostgreSQL
thành các bảng phân tích được quản lý bằng dbt.

## Data layers

### Staging

Các model `stg_*` chuẩn hóa tên cột, kiểu dữ liệu và metadata ingestion.
Grain của dữ liệu không bị thay đổi so với source.

### Warehouse

Các dimension hiện lưu trạng thái mới nhất của khách hàng, danh mục và
sản phẩm theo mô hình SCD Type 1.

Các fact table gồm:

- `fct_orders`: một dòng cho mỗi đơn hàng.
- `fct_order_items`: một dòng cho mỗi sản phẩm trong đơn hàng.

Giá bán lịch sử được lấy từ `fct_order_items.unit_price`, không lấy từ
giá hiện tại trong dimension sản phẩm.

### Marts

Các marts phục vụ phân tích:

- `mart_daily_sales`: doanh thu theo ngày và loại tiền.
- `mart_customer_sales`: doanh số theo khách hàng và loại tiền.
- `mart_product_sales`: doanh số theo sản phẩm và loại tiền.

`currency_code` luôn thuộc grain của các bảng tổng hợp để tránh cộng
lẫn những loại tiền khác nhau.

## Revenue definitions

- `gross_order_amount`: tổng giá trị đơn hàng ở mọi trạng thái.
- `delivered_order_amount`: tổng giá trị các đơn đã giao.
- `gross_sales_amount`: tổng giá trị các dòng sản phẩm ở mọi trạng thái.
- `delivered_sales_amount`: tổng giá trị dòng sản phẩm thuộc đơn đã giao.

{% enddocs %}
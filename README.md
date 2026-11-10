# BÀI TẬP VỀ NHÀ BÀI 14: XÂY DỰNG DATA PIPELINE KẾT HỢP APACHE AIRFLOW VÀ APACHE SPARK

Dự án Data Engineering tích hợp giữa **Apache Airflow** (vai trò Điều phối - Orchestration) và **Apache Spark (PySpark)** (vai trò Xử lý & Biến đổi dữ liệu phân tán - Distributed Data Processing). 

Dự án hiện thực hóa trọn vẹn yêu cầu bài toán từ Lecture 10 và nâng cấp toàn diện các công đoạn xử lý (ETL) từ Python/Bash Operator sang **PySpark** kết hợp với **`SparkSubmitOperator`**.

---

## 1. Kiến trúc hệ thống (System Architecture)

```
                            +-----------------------------------------------+
                            |             APACHE AIRFLOW CLUSTER            |
                            |                                               |
                            |   +------------------+    +---------------+   |
                            |   | Airflow Webserver|    |Airflow Init   |   |
                            |   | (Port 8080)      |    | & Migrations  |   |
                            |   +--------+---------+    +---------------+   |
                            |            |                                  |
                            |   +--------v---------+                        |
                            |   |Airflow Scheduler |                        |
                            |   | (LocalExecutor)  |                        |
                            |   +--------+---------+                        |
                            +------------|----------------------------------+
                                         |  SparkSubmitOperator (Job Submissions)
                                         v
+----------------------------------------+----------------------------------+
|                   APACHE SPARK CLUSTER (Bitnami 3.4.1)                    |
|                                                                           |
|   +------------------------------------+   +--------------------------+   |
|   | Spark Master (Port 7077 RPC)       |-->| Spark Worker             |   |
|   | Web UI: http://localhost:8081      |   | Web UI: localhost:8082   |   |
|   +------------------------------------+   +--------------------------+   |
+----------------------------------------+----------------------------------+
                                         |
                                         | JDBC Writes (.mode("append"))
                                         v
+---------------------------------------------------------------------------+
|                      POSTGRESQL DATABASE (Port 5432)                      |
|                                                                           |
|   - Airflow Metadata Backend                                              |
|   - Destination Warehouse Tables:                                         |
|       * bitcoin_rates                                                     |
|       * male_users                                                        |
|       * female_users                                                      |
|       * dnd_characters                                                    |
+---------------------------------------------------------------------------+
```

### Các thành phần chính:
1. **Airflow Cluster**:
   - **Airflow Webserver**: Giao diện điều khiển UI tại cổng `8080`.
   - **Airflow Scheduler**: Quản lý lịch trình, kích hoạt DAGs và submit các PySpark jobs tới Spark Cluster.
   - **Airflow Init**: Tự động migrate cơ sở dữ liệu, khởi tạo tài khoản `admin/admin` và cấu hình sẵn connection `spark_default` và `postgres_default`.
2. **Spark Cluster (Bitnami Spark 3.4.1)**:
   - **Spark Master**: Quản lý tài nguyên cluster, Web UI tại cổng `8081` (`http://localhost:8081`).
   - **Spark Worker**: Thực thi các tasks phân tán do Airflow submit sang.
3. **PostgreSQL Database**:
   - Vừa lưu trữ Airflow Metadata, vừa đóng vai trò Destination Data Warehouse cho các bảng kết quả của PySpark thông qua Spark JDBC.

---

## 2. Danh mục 3 Data Pipelines (DAGs)

### 📌 DAG 1: `first_dag` (Bitcoin API -> PySpark Parse JSON -> Trích xuất tỷ giá USD)
* **Luồng xử lý**:
  1. `create_directories`: Tạo thư mục lưu trữ dữ liệu thô `/opt/airflow/data/bitcoin`.
  2. `download_bitcoin_data`: Tải dữ liệu JSON thời gian thực về giá Bitcoin (kèm cơ chế high-availability fallback nếu DNS CoinDesk bị gián đoạn).
  3. `process_bitcoin_spark` (`SparkSubmitOperator`):
     - PySpark đọc tập tin JSON đa dòng (`multiline=True`).
     - Trích xuất tỷ giá `bpi.USD.rate_float`, đơn vị tiền tệ, mô tả và thời điểm cập nhật.
     - Hiển thị tỷ giá USD rõ ràng trên log Driver và dùng Spark JDBC để lưu trữ vào bảng `bitcoin_rates`.

### 📌 DAG 2: `second_dag` (RandomUser API -> PySpark Phân nhánh Giới tính -> PostgreSQL)
* **Luồng xử lý**:
  1. `create_directories`: Khởi tạo thư mục `/opt/airflow/data/random_user`.
  2. `download_random_users`: Tải 100 hồ sơ người dùng từ RandomUser API dạng JSON lồng nhau.
  3. `process_random_users_spark` (`SparkSubmitOperator`):
     - PySpark bung mảng (`explode`) và làm phẳng (`flatten`) các trường: `user_id`, `full_name`, `email`, `phone`, `dob`, `age`, `country`, `nationality`.
     - **Phân nhánh (Branching)**: Tách luồng thành 2 DataFrames riêng biệt: `male_df` và `female_df`.
     - Tính toán thống kê độ tuổi trung bình theo giới tính.
     - Dùng Spark JDBC nạp đồng thời:
       - Nam vào bảng `male_users`.
       - Nữ vào bảng `female_users`.

### 📌 DAG 3: `dnd_weekly_characters` (D&D 5e API -> PySpark Tạo ngẫu nhiên nhân vật -> PostgreSQL)
* **Lịch chạy**: Định kỳ hàng tuần (`@weekly`).
* **Luồng xử lý**:
  1. `create_directories`: Khởi tạo thư mục `/opt/airflow/data/dnd`.
  2. `fetch_dnd_api_metadata`: Giao tiếp với REST API của D&D 5e để lấy danh mục các Chủng tộc (`races`), Nghề nghiệp (`classes`), và Phẩm chất tư tưởng (`alignments`).
  3. `generate_characters_spark` (`SparkSubmitOperator`):
     - PySpark sinh 50 nhân vật ngẫu nhiên trên phân vùng phân tán.
     - Đặt tên nhân vật dạng huyền bí, gán Race, Class, Alignment.
     - Đổ chỉ số cơ bản (Ability Scores: STR, DEX, CON, INT, WIS, CHA) từ 3 đến 18.
     - Tính toán máu khởi điểm (`hit_points`) theo đúng luật D&D 5e dựa trên Hit Die của Class và điểm cộng Thể chất (CON modifier).
     - Gom nhóm phân tích phân bố Class và ghi dữ liệu vào bảng `dnd_characters` qua Spark JDBC.

---

## 3. Cấu trúc thư mục dự án

```
lab14/
│
├── dags/                                 # Airflow DAGs
│   ├── first_dag.py                     # DAG 1: Bitcoin Price ETL
│   ├── second_dag.py                    # DAG 2: RandomUser Branching ETL
│   └── dnd_weekly_characters.py         # DAG 3: D&D 5e Weekly Characters ETL
│
├── spark_jobs/                           # Mã nguồn PySpark thực thi trên Spark Cluster
│   ├── process_bitcoin.py               # Xử lý parse JSON Bitcoin & ghi DB
│   ├── process_random_users.py          # Bung mảng, phân nhánh nam/nữ & ghi DB
│   └── process_dnd_characters.py        # Thuật toán sinh nhân vật phân tán & ghi DB
│
├── sql/
│   └── init_db.sql                      # DDL tạo sẵn schema và bảng trong PostgreSQL
│
├── jars/
│   └── postgresql-42.6.0.jar            # PostgreSQL JDBC Driver cho Spark
│
├── data/                                # Shared Volume lưu trữ JSON trung gian
│   ├── bitcoin/
│   ├── random_user/
│   └── dnd/
│
├── Dockerfile                           # Custom Airflow image kèm Java 17 & Spark 3.4.1 Client
├── docker-compose.yaml                  # Cấu hình cụm Airflow, Spark Cluster và Postgres
├── requirements.txt                     # Thư viện Python phụ trợ
├── .env                                 # Biến môi trường Airflow UID
└── README.md                            # Hướng dẫn chi tiết
```

---

## 4. Hướng dẫn cài đặt và vận hành

### Bước 1: Khởi tạo môi trường & Database migration
Mở PowerShell tại thư mục dự án và chạy:
```powershell
docker-compose up airflow-init
```
*Lệnh này sẽ khởi chạy database PostgreSQL, tự động migrate schema Airflow, tạo user admin (`admin`/`admin`) và thiết lập sẵn connection `spark_default`.*

### Bước 2: Khởi động toàn bộ Cụm Container
Chạy chế độ nền (Detached mode):
```powershell
docker-compose up -d
```

Kiểm tra trạng thái các container:
```powershell
docker ps
```
Bạn sẽ thấy 5 container hoạt động ổn định:
- `airflow-webserver`
- `airflow-scheduler`
- `spark-master`
- `spark-worker`
- `postgres`

### Bước 3: Truy cập Giao diện Web
1. **Airflow Web UI**: Truy cập [http://localhost:8080](http://localhost:8080)
   - **Tài khoản**: `admin`
   - **Mật khẩu**: `admin`
   - Bật (Unpause) và nhấn nút **Trigger DAG** (nút Play) để chạy lần lượt:
     - `first_dag`
     - `second_dag`
     - `dnd_weekly_characters`

2. **Spark Master UI**: Truy cập [http://localhost:8081](http://localhost:8081)
   - Theo dõi trạng thái Spark Cluster.
   - Khi Airflow kích hoạt task Spark, bạn sẽ thấy các ứng dụng xuất hiện trong mục **Completed Applications** / **Running Applications**:
     - `Bitcoin_USD_Rate_Extraction`
     - `RandomUser_Gender_Branching_ETL`
     - `DnD_Weekly_Character_Generation`

3. **Spark Worker UI**: Truy cập [http://localhost:8082](http://localhost:8082)
   - Xem chi tiết tài nguyên CPU / RAM mà Worker đã phân bổ cho các Job.

---

## 5. Kiểm tra kết quả trong Database PostgreSQL

Sau khi các DAG chạy thành công, bạn có thể kiểm tra dữ liệu đã nạp vào cơ sở dữ liệu bằng cách truy cập container Postgres:

```powershell
docker exec -it postgres psql -U airflow -d airflow
```

Chạy các câu lệnh SQL sau:

```sql
-- 1. Xem dữ liệu tỷ giá Bitcoin vừa nạp từ first_dag
SELECT id, currency, usd_rate, updated_time, processed_at 
FROM bitcoin_rates 
ORDER BY processed_at DESC 
LIMIT 5;

-- 2. Kiểm tra số lượng người dùng theo giới tính từ second_dag
SELECT 'male_users' AS cohort, COUNT(*) AS total_count FROM male_users
UNION ALL
SELECT 'female_users' AS cohort, COUNT(*) AS total_count FROM female_users;

-- 3. Xem danh sách nhân vật D&D được sinh từ dnd_weekly_characters
SELECT character_name, race, character_class, alignment, level, hit_points, strength, dexterity 
FROM dnd_characters 
ORDER BY id DESC 
LIMIT 10;

-- 4. Thống kê phân bố chủng tộc trong D&D
SELECT race, COUNT(*) AS character_count, ROUND(AVG(hit_points), 1) AS avg_hp
FROM dnd_characters
GROUP BY race
ORDER BY character_count DESC;

-- Thoát psql
\q
```

---

## 6. Dừng hệ thống

Khi hoàn tất bài tập hoặc muốn giải phóng tài nguyên RAM/CPU:
```powershell
docker-compose down
```
*(Nếu muốn xóa sạch cả dữ liệu database để chạy lại từ đầu: `docker-compose down -v`)*

# BÁO CÁO BÀI TẬP VỀ NHÀ LECTURE 14
## XÂY DỰNG ETL PIPELINE KẾT HỢP APACHE AIRFLOW VÀ APACHE SPARK

**Học phần**: Data Engineering  
**Học viên**: Đào Quang Huy  
**Môi trường thực thi**: Apache Airflow 2.7.3 | Apache Spark 3.4.1 (Bitnami) | PostgreSQL 13 | PySpark 3.4.1 | OpenJDK 17 LTS | Docker Compose  

---

## 1. TỔNG QUAN BÀI TOÁN & MỤC TIÊU DỰ ÁN

Bài tập thực hành nâng cao Lecture 14 yêu cầu xây dựng một hệ thống **Data Pipeline hoàn chỉnh chuẩn End-to-End** kết hợp giữa hai công nghệ lõi trong kỹ thuật dữ liệu hiện đại:
1. **Apache Airflow**: Đóng vai trò là nền tảng **Điều phối quy trình (Orchestration Engine)**, chịu trách nhiệm quản lý lịch trình (Scheduling), theo dõi phụ thuộc (Dependency Management), bắt lỗi và giám sát trạng thái các tác vụ.
2. **Apache Spark (PySpark)**: Đóng vai trò là nền tảng **Xử lý dữ liệu phân tán (Distributed Processing Engine)**, tiếp nhận các công việc xử lý dữ liệu nặng (Trích xuất, Làm sạch, Biến đổi đa chiều, Phân nhánh, và Nạp dữ liệu qua Spark JDBC).

### Nâng cấp từ Lecture 10 lên Lecture 14:
Trong bài thực hành Lecture 10, các luồng dữ liệu (Bitcoin API, RandomUser API, D&D API) được xử lý cục bộ bằng các toán tử đơn giản như `BashOperator` hoặc `PythonOperator` (sử dụng thư viện chuẩn `requests`, `json`, và nạp trực tiếp vào cơ sở dữ liệu trên single process). 

Ở Lecture 14, toàn bộ logic ETL/ELT đã được chuyển dịch hoàn toàn sang **PySpark** và được gửi tới cụm Spark độc lập thông qua **`SparkSubmitOperator`**, mang lại các ưu thế vượt trội:
* **Tách biệt hạ tầng (Decoupling)**: Airflow Scheduler chỉ điều phối mà không tiêu tốn tài nguyên CPU/RAM để xử lý dữ liệu.
* **Xử lý phân tán (Distributed Computation)**: Toàn bộ công đoạn biến đổi JSON, bung mảng (Explode), phân nhánh và tính toán thống kê chạy trên cụm Spark (Spark Master & Spark Workers).
* **Tối ưu hóa nạp dữ liệu (High-throughput JDBC)**: Nạp song song vào PostgreSQL bằng Spark JDBC Driver.

---

## 2. KIẾN TRÚC HỆ THỐNG (SYSTEM ARCHITECTURE)

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
                                         |  SparkSubmitOperator (spark-submit RPC)
                                         v
+----------------------------------------+----------------------------------+
|                   APACHE SPARK CLUSTER (Bitnami 3.4.1)                    |
|                                                                           |
|   +------------------------------------+   +--------------------------+   |
|   | Spark Master (Port 7077 RPC)       |-->| Spark Worker (Executors) |   |
|   | Web UI: http://localhost:8081      |   | Web UI: localhost:8082   |   |
|   +------------------------------------+   +--------------------------+   |
+----------------------------------------+----------------------------------+
                                         |
                                         | Spark JDBC (.mode("append"))
                                         v
+---------------------------------------------------------------------------+
|                      POSTGRESQL DATABASE (Port 5432)                      |
|                                                                           |
|   - Airflow Metadata Repository                                           |
|   - Target Data Warehouse Tables:                                         |
|       * bitcoin_rates       (Lưu trữ tỷ giá BTC/USD theo thời gian)       |
|       * male_users          (Hồ sơ người dùng Nam)                        |
|       * female_users        (Hồ sơ người dùng Nữ)                         |
|       * dnd_characters      (Danh sách nhân vật D&D được sinh ngẫu nhiên) |
+---------------------------------------------------------------------------+
```

### Các dịch vụ trong Docker Compose:
1. **`postgres` (PostgreSQL 13)**: Cung cấp kho dữ liệu cho Airflow metadata và các bảng nghiệp vụ (`bitcoin_rates`, `male_users`, `female_users`, `dnd_characters`). Tự động thực thi kịch bản DDL `sql/init_db.sql` khi khởi động.
2. **`airflow-init`**: Container chạy một lần để khởi tạo Airflow Database (`airflow db migrate`), đăng ký tài khoản quản trị `admin`/`admin`, và tự động thiết lập 2 kết nối chuẩn:
   - `spark_default`: `spark://spark-master:7077`
   - `postgres_default`: `postgresql://airflow:airflow@postgres:5432/airflow`
3. **`airflow-webserver`**: Phục vụ giao diện người dùng tại `http://localhost:8080`.
4. **`airflow-scheduler`**: Lập lịch và điều phối các tác vụ, thực thi `SparkSubmitOperator` để gửi PySpark jobs sang Spark Master.
5. **`spark-master`**: Điều phối tài nguyên cụm Spark, cung cấp Spark Web UI tại cổng `8081` (`http://localhost:8081`).
6. **`spark-worker`**: Node tính toán thực thi PySpark transformations và actions với 2 Cores và 2GB RAM.

---

## 3. CHI TIẾT THIẾT KẾ & HIỆN THỰC 3 DATA PIPELINES (DAGS)

### 3.1. DAG 1: `first_dag` - Trích xuất Tỷ giá Bitcoin từ CoinDesk API
* **Mục tiêu**: Tải dữ liệu JSON thời gian thực từ CoinDesk API, sử dụng PySpark DataFrame API để phân tích cấu trúc JSON lồng nhau, trích xuất tỷ giá quy đổi sang USD và lưu vết vào PostgreSQL.
* **Cấu trúc tác vụ trong DAG**:
  1. `create_directories`: Khởi tạo thư mục dữ liệu `/opt/airflow/data/bitcoin`.
  2. `download_bitcoin_data` (`PythonOperator`): Tải JSON tỷ giá BTC. Tích hợp cơ chế dự phòng tự động (Failover Fallback) kết nối sang API thời gian thực của Coinbase/CoinGecko nếu DNS `api.coindesk.com` gặp sự cố mạng bên ngoài.
  3. `process_bitcoin_spark` (`SparkSubmitOperator`):
     - Gọi kịch bản `spark_jobs/process_bitcoin.py`.
     - Đọc JSON với tùy chọn `multiline=True`.
     - Phép biến đổi:
       ```python
       df_extracted = df_raw.select(
           col("bpi.USD.code").alias("currency"),
           col("bpi.USD.description").alias("description"),
           col("bpi.USD.rate").alias("rate_str"),
           col("bpi.USD.rate_float").cast("decimal(15,4)").alias("usd_rate"),
           col("time.updated").alias("updated_time"),
           col("time.updatedISO").alias("updated_time_iso")
       ).withColumn("processed_at", current_timestamp())
       ```
     - Nạp vào PostgreSQL bảng `bitcoin_rates` qua Spark JDBC Driver.

---

### 3.2. DAG 2: `second_dag` - Phân nhánh Giới tính Dữ liệu RandomUser API
* **Mục tiêu**: Tải tập dữ liệu 100 người dùng đa quốc gia, dùng PySpark để bung mảng (Explode), làm phẳng cấu trúc lồng nhau (Flatten), phân nhánh theo giới tính (`male` / `female`), và nạp song song vào 2 bảng riêng biệt trong database.
* **Cấu trúc tác vụ trong DAG**:
  1. `create_directories`: Khởi tạo `/opt/airflow/data/random_user`.
  2. `download_random_users` (`PythonOperator`): Tải tập dữ liệu JSON từ `https://randomuser.me/api/?results=100`.
  3. `process_random_users_spark` (`SparkSubmitOperator`):
     - Gọi kịch bản `spark_jobs/process_random_users.py`.
     - Bung mảng `results` và làm phẳng các trường `name`, `location`, `dob`, `login`:
       ```python
       df_exploded = df_raw.select(explode(col("results")).alias("user"))
       # Làm phẳng và chuẩn hóa schema người dùng...
       ```
     - **Phân nhánh dữ liệu (Cohort Branching)**:
       ```python
       df_male = df_users.filter(col("gender") == "male")
       df_female = df_users.filter(col("gender") == "female")
       ```
     - **Tính toán kết tập (Aggregations)**: Tính độ tuổi trung bình theo từng giới tính bằng `groupBy("gender").agg(avg("age"))`.
     - **Nạp dữ liệu bằng JDBC**:
       - `df_male.write.jdbc(...)` ghi vào bảng `male_users`.
       - `df_female.write.jdbc(...)` ghi vào bảng `female_users`.

---

### 3.3. DAG 3: `dnd_weekly_characters` - Tạo ngẫu nhiên nhân vật D&D 5e
* **Lịch biểu**: Định kỳ hàng tuần (`@weekly`).
* **Mục tiêu**: Giao tiếp với API D&D 5e (`https://www.dnd5eapi.co/api/`) để lấy metadata tham chiếu (Races, Classes, Alignments), sau đó dùng PySpark để sinh tự động 50 nhân vật hoàn chỉnh với các chỉ số cân bằng theo luật D&D 5th Edition.
* **Cấu trúc tác vụ trong DAG**:
  1. `create_directories`: Khởi tạo `/opt/airflow/data/dnd`.
  2. `fetch_dnd_api_metadata` (`PythonOperator`): Gọi 3 endpoints của API: `/api/races`, `/api/classes`, `/api/alignments`.
  3. `generate_characters_spark` (`SparkSubmitOperator`):
     - Gọi kịch bản `spark_jobs/process_dnd_characters.py`.
     - Khởi tạo DataFrame phân tán: `spark.range(0, 50)`.
     - Sinh ngẫu nhiên tên gọi bằng cách ghép các tiền tố và hậu tố huyền ảo (Fantasy Prefix + Suffix).
     - Rải ngẫu nhiên Chủng tộc, Nghề nghiệp, Phẩm chất dựa trên mảng tham chiếu lấy từ API.
     - Đổ 6 chỉ số thuộc tính cơ bản (STR, DEX, CON, INT, WIS, CHA) theo phân phối 3d6 (giá trị từ 3 đến 18) bằng các hàm Spark SQL: `floor(rand() * 16 + 3)`.
     - **Tính toán Máu (Hit Points - HP) chuẩn D&D 5e**:
       $$\text{Hit Die} = \begin{cases} 12 & \text{nếu Class = Barbarian} \\ 10 & \text{nếu Class } \in \{\text{Fighter, Paladin, Ranger}\} \\ 8 & \text{nếu Class } \in \{\text{Cleric, Druid, Monk, Rogue, Warlock, Bard}\} \\ 6 & \text{nếu Class } \in \{\text{Wizard, Sorcerer}\} \end{cases}$$
       $$\text{CON Modifier} = \lfloor \frac{\text{CON} - 10}{2} \rfloor$$
       $$\text{Hit Points} = \max(6, (\text{Hit Die} + \text{CON Modifier}) + (\text{Level} - 1) \times (\frac{\text{Hit Die}}{2} + 1 + \text{CON Modifier}))$$
     - Thống kê phân bố lớp nhân vật và lượng máu trung bình theo Class.
     - Ghi 50 bản ghi nhân vật vào bảng `dnd_characters` trong PostgreSQL qua JDBC.

---

## 4. SO SÁNH KIẾN TRÚC: LECTURE 10 VS LECTURE 14

| Tiêu chí so sánh | Lecture 10 (Python/Bash Operator) | Lecture 14 (Airflow + SparkSubmitOperator) |
| :--- | :--- | :--- |
| **Môi trường thực thi** | Cục bộ trên Worker/Scheduler của Airflow (Single Node). | Cụm phân tán Apache Spark (Driver & Worker Cluster). |
| **Tài nguyên tính toán** | Chia sẻ trực tiếp CPU/RAM với Airflow Daemon, dễ gây nghẽn Scheduler. | Tách biệt hoàn toàn (Decoupled), Spark Workers gánh toàn bộ tải xử lý. |
| **Khả năng co giãn (Scalability)** | Giới hạn theo cấu hình 1 máy chủ, không xử lý được Big Data (> vài GB). | Khả năng mở rộng theo chiều ngang (Scale Out) thêm Worker không giới hạn. |
| **Tối ưu hóa biến đổi** | Xử lý tuần tự bằng Python loop / Pandas in-memory, nguy cơ tràn RAM (OOM). | Cơ chế **Lazy Evaluation** với bộ tối ưu hóa truy vấn **Catalyst Optimizer** của Spark. |
| **Cơ chế nạp dữ liệu** | Insert từng dòng (row-by-row) hoặc Batch Insert bằng psycopg2. | Song song hóa nạp dữ liệu (Parallel JDBC Write Partitioning) qua PostgreSQL JDBC Driver. |
| **Giám sát hiệu năng** | Chỉ xem được log text cơ bản trong Airflow Task log. | Xem trực quan biểu đồ DAG Stages, Skewness, Tasks và Executors trên **Spark UI (8081)**. |

---

## 5. HƯỚNG DẪN VẬN HÀNH & KẾT QUẢ KIỂM THỬ

### 5.1. Khởi tạo môi trường
```powershell
docker-compose up airflow-init
docker-compose up -d
```

### 5.2. Các địa chỉ truy cập
* **Airflow Web UI**: `http://localhost:8080` (Tài khoản: `admin` / Mật khẩu: `admin`)
* **Spark Master UI**: `http://localhost:8081`
* **Spark Worker UI**: `http://localhost:8082`
* **PostgreSQL Database**: `localhost:5432` (`user: airflow`, `password: airflow`, `db: airflow`)

### 5.3. Kiểm chứng kết quả trong PostgreSQL
Truy cập vào container PostgreSQL:
```powershell
docker exec -it postgres psql -U airflow -d airflow
```

Truy vấn kiểm tra:
```sql
-- Kiểm tra bảng bitcoin_rates
SELECT currency, usd_rate, updated_time, processed_at FROM bitcoin_rates LIMIT 3;

-- Kiểm tra phân nhánh giới tính
SELECT 'Nam (male_users)' AS phan_nhanh, COUNT(*) AS so_luong FROM male_users
UNION ALL
SELECT 'Nu (female_users)' AS phan_nhanh, COUNT(*) AS so_luong FROM female_users;

-- Kiểm tra nhân vật D&D 5e
SELECT character_name, race, character_class, level, hit_points, strength, dexterity 
FROM dnd_characters LIMIT 5;
```

---

## 6. KẾT LUẬN

Dự án đã giải quyết trọn vẹn và hoàn hảo 100% các yêu cầu của Bài tập về nhà bài 14:
1. Đã thiết lập cụm Docker Compose chuẩn doanh nghiệp gồm Airflow Cluster, Spark Master, Spark Worker (Bitnami Spark 3.4.1) và PostgreSQL 13.
2. Xây dựng hoàn chỉnh 3 DAGs: `first_dag`, `second_dag`, và `dnd_weekly_characters` tuân thủ đúng chuẩn kết nối `SparkSubmitOperator`.
3. Toàn bộ logic ETL/ELT được hiện thực hóa bằng PySpark chuyên nghiệp, tận dụng các kỹ thuật cốt lõi của Spark: Struct Schema, Explode, Filter/Branching, Aggregations, và nạp dữ liệu qua JDBC.
4. Dự án đạt độ tin cậy cao nhờ cơ chế tự động khởi tạo kết nối và kịch bản DDL database.

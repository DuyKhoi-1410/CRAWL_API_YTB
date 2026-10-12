# YouTube Data Crawling Pipeline (Bronze Layer)

Pipeline thu thập dữ liệu thô (Metadata Video & Comments) từ các kênh YouTube sử dụng YouTube Data API v3 và đẩy lên Google Cloud Storage (GCS) theo kiến trúc Medallion (Bronze Layer), sẵn sàng triển khai trên **Google Cloud Run Jobs**.

---

## 1. Cấu trúc thư mục

```text
CRAWL_API_YTB/
├── code_bigdata_CRAWL.py    # Script chính thực hiện cào dữ liệu & đẩy lên GCS
├── Dockerfile               # Cấu hình đóng gói container image (Python 3.11-slim)
├── requirements.txt         # Danh sách thư viện Python phụ thuộc
├── .dockerignore            # Loại trừ file rác khi build Docker image
├── .gitignore               # Loại trừ file nhạy cảm và cache khi commit Git
└── README.md                # Tài liệu hướng dẫn
```

---

## 2. Các biến môi trường (Environment Variables)

| Tên biến | Bắt buộc | Mặc định | Mô tả |
| :--- | :---: | :--- | :--- |
| `YOUTUBE_API_KEY` | **Có** | Không có | Khóa API từ Google Cloud Console có quyền gọi YouTube Data API v3. |
| `GCS_BUCKET_NAME` | Không | `bd-raw-data-bronze` | Tên GCS Bucket để lưu trữ dữ liệu thô và file state. |

---

## 3. Chạy thử nghiệm ở môi trường Local (Tùy chọn)

### Cách 1: Chạy trực tiếp bằng Python
```bash
# Cài đặt thư viện
pip install -r requirements.txt

# Thiết lập biến môi trường (Windows PowerShell)
$env:YOUTUBE_API_KEY="AIzaSy..."
$env:GCS_BUCKET_NAME="bd-raw-data-bronze"

# Chạy script
python code_bigdata_CRAWL.py
```

### Cách 2: Chạy thử bằng Docker
```bash
# Build image
docker build -t ytb-crawler:latest .

# Run container
docker run --rm -e YOUTUBE_API_KEY="AIzaSy..." -e GCS_BUCKET_NAME="bd-raw-data-bronze" ytb-crawler:latest
```

---

## 4. Hướng dẫn triển khai lên Google Cloud Run Jobs (Khi sẵn sàng đẩy lên GitHub)

### Bước 1: Đẩy mã nguồn lên GitHub
1. Khởi tạo Git và commit:
   ```bash
   git init
   git add .
   git commit -m "feat: setup ready for cloud run jobs"
   ```
2. Đẩy lên GitHub repository cá nhân:
   ```bash
   git remote add origin https://github.com/<username>/<repo-name>.git
   git branch -M main
   git push -u origin main
   ```

### Bước 2: Thiết lập Service Account trên Google Cloud
Gán quyền cho Service Account mà Cloud Run sử dụng:
- Role: **`Storage Object Admin`** (để đọc/ghi file `video_state.json` và tải lên các file `.jsonl` trong bucket `bd-raw-data-bronze`).

### Bước 3: Tạo Cloud Run Job từ GitHub
1. Vào **Google Cloud Console** > **Cloud Run** > chọn mục **Jobs** > **Create Job**.
2. Chọn **Continuously deploy new revisions from a source repository** > Thiết lập liên kết đến GitHub repo vừa tạo.
3. Trong mục **Build configuration**, chọn build type là **Dockerfile**.
4. Cấu hình biến môi trường:
   - Thêm `YOUTUBE_API_KEY`: Nhập key trực tiếp hoặc trỏ tới Google Secret Manager.
   - Thêm `GCS_BUCKET_NAME`: `bd-raw-data-bronze` (tùy chọn).
5. Thiết lập tài nguyên (Resource):
   - CPU: `1 vCPU`, Memory: `1 GiB` (hoặc `2 GiB`).
   - Task timeout: `3600s` (1 giờ) hoặc tăng tùy theo khối lượng cào.

### Bước 4: Thiết lập Cloud Scheduler (Tự động chạy định kỳ)
1. Vào **Cloud Scheduler** > **Create Schedule**.
2. **Frequency (Cron):**
   - Ví dụ: `0 2 * * *` (Chạy mỗi ngày một lần vào 02:00 UTC).
3. **Target Type:** Chọn **Cloud Run Job**.
4. Chọn Cloud Run Job vừa tạo ở Bước 3.

FROM python:3.11-slim

# Thiết lập thư mục làm việc trong container
WORKDIR /app

# Đảm bảo log Python được in ra ngay lập tức (hỗ trợ Cloud Logging hiển thị real-time)
ENV PYTHONUNBUFFERED=1

# Cài đặt các thư viện cần thiết trước để tận dụng Docker layer cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy toàn bộ mã nguồn vào container
COPY . .

# Lệnh thực thi chính khi container khởi chạy trên Cloud Run Jobs
CMD ["python", "code_bigdata_CRAWL.py"]

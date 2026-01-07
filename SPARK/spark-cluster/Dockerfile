FROM bitnami/spark:3.5.0

WORKDIR /app  # Đặt thư mục làm việc là /app trong container

COPY ./app.py /app/app.py

CMD ["spark-submit", "--master", "spark://spark-master:7077", "/app/app.py"]

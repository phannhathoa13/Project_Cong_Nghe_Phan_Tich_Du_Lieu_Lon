# Thư viện dùng để đọc file CSV
import csv

# Thư viện dùng để chuyển dữ liệu sang JSON
import json

# Thư viện dùng để tạo thời gian chờ
import time

# Thư viện dùng để gửi dữ liệu đến Kafka
from kafka import KafkaProducer


# Khởi tạo Kafka Producer
producer = KafkaProducer(
    # Địa chỉ Kafka Server
    bootstrap_servers="localhost:9092",

    # Chuyển dữ liệu Python thành JSON và mã hóa UTF-8
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


# Mở file dữ liệu taxi
with open(
    "data/train.csv",
    mode="r",
    encoding="utf-8"
) as file:

    # Đọc từng dòng CSV dưới dạng dictionary
    reader = csv.DictReader(file)

    # Duyệt qua từng dòng dữ liệu
    for i, row in enumerate(reader):

        # Tạo dữ liệu chuyến đi để gửi đến Kafka
        data = {
            "id": row["id"],
            "vendor_id": int(row["vendor_id"]),
            "pickup_datetime": row["pickup_datetime"],
            "dropoff_datetime": row["dropoff_datetime"],
            "passenger_count": int(row["passenger_count"]),
            "pickup_longitude": float(row["pickup_longitude"]),
            "pickup_latitude": float(row["pickup_latitude"]),
            "dropoff_longitude": float(row["dropoff_longitude"]),
            "dropoff_latitude": float(row["dropoff_latitude"]),
            "trip_duration": int(row["trip_duration"])
        }

        # Gửi dữ liệu đến topic taxi_trips
        producer.send(
            "taxi_trips",
            value=data
        )

        # Hiển thị ID của chuyến đi đã gửi
        print(
            "Đã gửi:",
            data["id"]
        )

        # Chờ 0,2 giây trước khi gửi dòng tiếp theo
        time.sleep(0.2)

        # Dừng sau khi gửi đủ 100 dòng dữ liệu
        if i >= 99:
            break


# Đảm bảo toàn bộ dữ liệu đã được gửi đi
producer.flush()

# Đóng kết nối Kafka Producer
producer.close()

# Thông báo hoàn thành
print("Đã gửi xong dữ liệu!")
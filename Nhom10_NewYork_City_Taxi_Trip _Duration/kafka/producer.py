import csv
import json
import time

from kafka import KafkaProducer


producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


with open(
    "data/train.csv",
    mode="r",
    encoding="utf-8"
) as file:

    reader = csv.DictReader(file)

    for i, row in enumerate(reader):

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

        producer.send(
            "taxi_trips",
            value=data
        )

        print(
            "Đã gửi:",
            data["id"]
        )

        time.sleep(0.2)

        if i >= 99:
            break


producer.flush()
producer.close()

print("Đã gửi xong dữ liệu!")
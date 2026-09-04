import os
import psycopg2

# 1. Cấu hình môi trường

os.environ["JAVA_HOME"] = (
    r"C:\Users\beste\Downloads\jdk-17.0.12_windows-x64_bin\jdk-17.0.12"
)

os.environ["HADOOP_HOME"] = r"C:\hadoop"


# 2. Import Spark

from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    radians,
    sin,
    cos,
    asin,
    sqrt,
    floor,
    concat_ws,
    window,
    count,
    avg,
    when,
    hour,
    dayofweek,
    month
)

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressionModel


# 3. Khởi tạo SparkSession

spark = SparkSession.builder \
    .appName("TrafficStreaming") \
    .master("local[2]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# 4. Load Machine Learning Model

ml_model = RandomForestRegressionModel.load(
    "model/traffic_rf_model"
)


# 5. Định nghĩa Schema JSON

schema = StructType([
    StructField("id", StringType(), True),
    StructField("vendor_id", IntegerType(), True),
    StructField("pickup_datetime", StringType(), True),
    StructField("dropoff_datetime", StringType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("pickup_longitude", DoubleType(), True),
    StructField("pickup_latitude", DoubleType(), True),
    StructField("dropoff_longitude", DoubleType(), True),
    StructField("dropoff_latitude", DoubleType(), True),
    StructField("trip_duration", IntegerType(), True)
])


# 6. Đọc dữ liệu từ Kafka

kafka_df = spark.readStream \
    .format("kafka") \
    .option(
        "kafka.bootstrap.servers",
        "localhost:9092"
    ) \
    .option(
        "subscribe",
        "taxi_trips"
    ) \
    .option(
        "startingOffsets",
        "latest"
    ) \
    .load()


# 7. Lấy JSON và timestamp Kafka

json_df = kafka_df.selectExpr(
    "CAST(value AS STRING) AS json_value",
    "timestamp AS event_time"
)


# 8. Parse JSON

taxi_df = json_df.select(
    from_json(
        col("json_value"),
        schema
    ).alias("data"),

    col("event_time")
).select(
    "data.*",
    "event_time"
)


# 9. Chuyển thời gian

taxi_df = taxi_df.withColumn(
    "pickup_datetime",
    to_timestamp("pickup_datetime")
)


# 10. Lọc dữ liệu không hợp lệ

taxi_df = taxi_df.filter(
    (col("trip_duration") > 0) &
    (col("pickup_latitude").between(40.5, 41.0)) &
    (col("pickup_longitude").between(-74.3, -73.6)) &
    (col("dropoff_latitude").between(40.5, 41.0)) &
    (col("dropoff_longitude").between(-74.3, -73.6))
)


# 11. Tính khoảng cách Haversine

lat1 = radians(
    col("pickup_latitude")
)

lon1 = radians(
    col("pickup_longitude")
)

lat2 = radians(
    col("dropoff_latitude")
)

lon2 = radians(
    col("dropoff_longitude")
)


a = (
    sin((lat2 - lat1) / 2) ** 2
    +
    cos(lat1) *
    cos(lat2) *
    sin((lon2 - lon1) / 2) ** 2
)


distance = (
    6371 *
    2 *
    asin(
        sqrt(a)
    )
)


taxi_df = taxi_df.withColumn(
    "distance_km",
    distance
)


# 12. Tính tốc độ trung bình

taxi_df = taxi_df.withColumn(
    "speed_kmh",

    col("distance_km") /
    (
        col("trip_duration") / 3600
    )
)


# Lọc tốc độ bất thường
taxi_df = taxi_df.filter(
    (col("distance_km") > 0) &
    (col("speed_kmh") > 0) &
    (col("speed_kmh") <= 120)
)


# 13. Tạo Grid

taxi_df = taxi_df \
    .withColumn(
        "grid_lat",
        floor(
            col("pickup_latitude") * 100
        )
    ) \
    .withColumn(
        "grid_lon",
        floor(
            col("pickup_longitude") * 100
        )
    )


taxi_df = taxi_df.withColumn(
    "grid_id",

    concat_ws(
        "_",
        col("grid_lat"),
        col("grid_lon")
    )
)


# 14. Gom dữ liệu theo Grid
#     và cửa sổ 5 phút

traffic_stream = taxi_df \
    .withWatermark(
        "event_time",
        "10 minutes"
    ) \
    .groupBy(
        window(
            col("event_time"),
            "5 minutes"
        ),

        col("grid_id")
    ) \
    .agg(
        count("*").alias(
            "trip_count"
        ),

        avg(
            "speed_kmh"
        ).alias(
            "avg_speed"
        ),

        avg(
            "trip_duration"
        ).alias(
            "avg_duration"
        ),

        avg(
            "distance_km"
        ).alias(
            "avg_distance"
        ),

        avg(
            "pickup_latitude"
        ).alias(
            "latitude"
        ),

        avg(
            "pickup_longitude"
        ).alias(
            "longitude"
        )
    )


# 15. Phân loại Traffic hiện tại

traffic_stream = traffic_stream.withColumn(
    "traffic_level",

    when(
        (col("trip_count") >= 10) &
        (col("avg_speed") < 10),

        "HIGH"
    )

    .when(
        (col("trip_count") >= 5) |
        (col("avg_speed") < 20),

        "MEDIUM"
    )

    .otherwise(
        "LOW"
    )
)


# 16. Tạo feature thời gian cho ML

traffic_stream = traffic_stream \
    .withColumn(
        "hour",
        hour(
            col("window.start")
        )
    ) \
    .withColumn(
        "day_of_week",
        dayofweek(
            col("window.start")
        )
    ) \
    .withColumn(
        "month",
        month(
            col("window.start")
        )
    )


# 17. Tạo Vector features

feature_columns = [
    "hour",
    "day_of_week",
    "month",
    "trip_count",
    "avg_speed",
    "avg_duration",
    "avg_distance",
    "latitude",
    "longitude"
]


assembler = VectorAssembler(
    inputCols=feature_columns,
    outputCol="features"
)


traffic_ml_stream = assembler.transform(
    traffic_stream
)


# 18. Machine Learning Prediction

prediction_stream = ml_model.transform(
    traffic_ml_stream
)


# 19. Phân loại traffic dự đoán

prediction_stream = prediction_stream.withColumn(
    "predicted_traffic_level",

    when(
        col("prediction") < 10,

        "HIGH"
    )

    .when(
        col("prediction") < 20,

        "MEDIUM"
    )

    .otherwise(
        "LOW"
    )
)


# 20. Chọn dữ liệu kết quả

result = prediction_stream.select(
    "window",
    "grid_id",
    "trip_count",
    "avg_speed",
    "avg_duration",
    "avg_distance",
    "traffic_level",
    "latitude",
    "longitude",

    col(
        "prediction"
    ).alias(
        "predicted_speed"
    ),

    "predicted_traffic_level"
)


# 21. Hàm lưu PostgreSQL/PostGIS

def save_to_postgres(
    batch_df,
    batch_id
):

    rows = batch_df.collect()

    if len(rows) == 0:
        return


    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="traffic_db",
        user="postgres",
        password="123"
    )


    cursor = conn.cursor()


    for row in rows:

        cursor.execute(
            """
            INSERT INTO traffic_data (
                window_start,
                window_end,
                grid_id,
                trip_count,
                avg_speed,
                traffic_level,
                latitude,
                longitude,
                geom,
                predicted_speed,
                predicted_traffic_level
            )

            VALUES (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,

                ST_SetSRID(
                    ST_MakePoint(
                        %s,
                        %s
                    ),
                    4326
                ),

                %s,
                %s
            )
            """,

            (
                row["window"]["start"],
                row["window"]["end"],

                row["grid_id"],

                row["trip_count"],

                row["avg_speed"],

                row["traffic_level"],

                row["latitude"],
                row["longitude"],

                # Longitude trước
                row["longitude"],

                # Latitude sau
                row["latitude"],

                row["predicted_speed"],

                row[
                    "predicted_traffic_level"
                ]
            )
        )


    conn.commit()

    cursor.close()

    conn.close()


    print(
        "Saved batch",
        batch_id,
        "to PostgreSQL"
    )


# 22. Chạy Spark Streaming

query = result.writeStream \
    .outputMode("update") \
    .foreachBatch(
        save_to_postgres
    ) \
    .start()


print(
    "======================================"
)

print(
    "Spark Streaming is processing traffic..."
)

print(
    "======================================"
)


query.awaitTermination()
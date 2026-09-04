import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="traffic_db",
    user="postgres",
    password="123"
)

print("Kết nối PostgreSQL thành công!")

cursor = conn.cursor()

cursor.execute("SELECT PostGIS_Version();")

version = cursor.fetchone()

print("PostGIS version:", version[0])

cursor.close()
conn.close()
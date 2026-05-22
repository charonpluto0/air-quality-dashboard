import pandas as pd
import json
import time

from kafka import KafkaProducer

# =====================================================
# Kafka Producer
# =====================================================

producer = KafkaProducer(

    bootstrap_servers='192.168.198.129:9092',

    value_serializer=lambda v:
        json.dumps(v).encode('utf-8')
)

# =====================================================
# 读取 CSV
# =====================================================

df = pd.read_csv("AirQuality_Cleaned.csv")

# =====================================================
# 逐条发送（只发送一次）
# =====================================================

for _, row in df.iterrows():

    try:

        co = float(row["CO(GT)"])
        no2 = float(row["NO2(GT)"])
        temp = float(row["T"])
        humidity = float(row["RH"])

    except:

        continue

    data = {

        "station_id": "ST_001",

        "measurement_time":
            str(row["Date"]) + " " + str(row["Time"]),

        "co": co,

        "no2": no2,

        "temp": temp,

        "humidity": humidity
    }

    producer.send(
        "air-quality-final",
        value=data
    )

    print(data)

    producer.flush()

    # 控制流速
    time.sleep(0.05)

print("========== 数据发送完成 ==========")
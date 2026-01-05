import time
import json
import random
from kafka import KafkaProducer
from datetime import datetime
from faker import Faker

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC_NAME = 'city_traffic'
NUM_TAXIS = 500

fake = Faker()


ROUTE_PATHS = [
    # 1. AVRUPA SAHİL YOLU (Eminönü -> Besiktas -> Ortakoy -> Bebek -> Sariyer)
    [
        (41.0170, 28.9770),  # Eminonu
        (41.0260, 28.9730),  # Karakoy
        (41.0320, 28.9880),  # Kabatas
        (41.0420, 29.0060),  # Besiktas
        (41.0480, 29.0250),  # Ortakoy
        (41.0600, 29.0350),  # Arnavutkoy
        (41.0750, 29.0440),  # Bebek
        (41.1100, 29.0550),  # Emirgan
        (41.1400, 29.0600),  # Tarabya
        (41.1660, 29.0500),  # Sariyer
    ],

    # 2. E-5 AVRUPA (Topkapi -> Bakirkoy -> Avcilar -> Beylikduzu)
    [
        (41.0150, 28.9350),  # Topkapi
        (41.0000, 28.9000),  # Merter
        (40.9900, 28.8600),  # Sirinevler
        (40.9850, 28.8000),  # Kucukcekmece
        (40.9950, 28.7200),  # Avcilar
        (41.0060, 28.6470),  # Beylikduzu
    ],

    # 3. 15 TEMMUZ KÖPRÜSÜ VE BAĞLANTILARI (Zincirlikuyu -> Altunizade)
    [
        (41.0660, 29.0130),  # Zincirlikuyu
        (41.0455, 29.0330),  # Kopru Avrupa Ayagi
        (41.0380, 29.0500),  # Kopru Anadolu Ayagi (Beylerbeyi ustu)
        (41.0250, 29.0400),  # Altunizade
    ],

    # 4. FSM KÖPRÜSÜ (Maslak -> Kavacik)
    [
        (41.1080, 29.0170),  # Maslak
        (41.0910, 29.0610),  # FSM Koprusu
        (41.0900, 29.0900),  # Kavacik
    ],

    # 5. ANADOLU SAHİL YOLU (Uskudar -> Kadikoy -> Bostanci -> Pendik)
    [
        (41.0250, 29.0150),  # Uskudar
        (41.0100, 29.0050),  # Harem
        (40.9900, 29.0250),  # Kadikoy
        (40.9700, 29.0400),  # Fenerbahce
        (40.9600, 29.0700),  # Caddebostan
        (40.9550, 29.0950),  # Bostanci
        (40.9300, 29.1400),  # Maltepe
        (40.8900, 29.1900),  # Kartal
        (40.8750, 29.2300),  # Pendik
    ],

    # 6. TEM OTOYOLU (Mahmutbey -> Seyrantepe)
    [
        (41.0550, 28.8000),  # Mahmutbey
        (41.0650, 28.8800),  # Gaziosmanpasa
        (41.0800, 28.9500),  # Hasdal
        (41.1000, 28.9900),  # Seyrantepe
    ],

    # 7. MİNİBÜS CADDESİ (Kadıköy - Pendik İç Hat)
    [
        (40.9900, 29.0350),  # Söğütlüçeşme
        (40.9850, 29.0550),  # Göztepe
        (40.9750, 29.0850),  # Kozyatağı
        (40.9650, 29.1100),  # Bostancı Köprüsü
    ]
]


def create_producer():
    print("Connecting to Kafka...")
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
            print("SUCCESS: Kafka connection established! 🚀")
            return producer
        except Exception as e:
            print(f"Waiting for connection... ({e})")
            time.sleep(3)


def get_location_on_rails():

    selected_path = random.choice(ROUTE_PATHS)

    segment_index = random.randint(0, len(selected_path) - 2)

    p1 = selected_path[segment_index]  # start point
    p2 = selected_path[segment_index + 1]  # end point

    ratio = random.random()
    lat = p1[0] + (p2[0] - p1[0]) * ratio
    lon = p1[1] + (p2[1] - p1[1]) * ratio


    lat += random.uniform(-0.0003, 0.0003)
    lon += random.uniform(-0.0003, 0.0003)

    return lat, lon


def generate_taxi_data(taxi_id):
    lat, lon = get_location_on_rails()

    # Trafik Durumu: Rastgele
    if random.random() < 0.3:
        speed = random.uniform(5, 25)
        status = "TRAFFIC_JAM"
    else:
        speed = random.uniform(40, 90)
        status = "FLOWING"

    return {
        "taxi_id": taxi_id,
        "event_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "lat": lat,
        "lon": lon,
        "speed": round(speed, 2),
        "status": status
    }


if __name__ == "__main__":
    producer = create_producer()
    taxi_ids = [fake.license_plate().replace(" ", "") for _ in range(NUM_TAXIS)]

    print(f"Rail-Based Simulation Started! {NUM_TAXIS} taxis constrained to real road geometry...")

    try:
        while True:
            for t_id in taxi_ids:
                data = generate_taxi_data(t_id)
                producer.send(TOPIC_NAME, value=data)

            producer.flush()

            # Hizli aksin
            time.sleep(0.002)

    except KeyboardInterrupt:
        producer.close()
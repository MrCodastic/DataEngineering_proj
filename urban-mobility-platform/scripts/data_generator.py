import time
import json
import random
import psycopg2
from kafka import KafkaProducer
from faker import Faker

# --- CONFIGURATION ---
KAFKA_TOPIC = "vehicle_gps"
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
DB_HOST = "localhost"
DB_NAME = "mobility_db"
DB_USER = "user"
DB_PASS = "password"

fake = Faker()

# 1. SETUP KAFKA PRODUCER
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# 2. SETUP POSTGRES CONNECTION
def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS
    )

def create_table():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            address VARCHAR(200),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()
    cur.close()
    conn.close()

# 3. GENERATORS
def generate_user():
    """Simulates a new user signing up to the app (Batch Data)"""
    conn = get_db_connection()
    cur = conn.cursor()
    name = fake.name()
    address = fake.address().replace('\n', ', ')
    
    cur.execute("INSERT INTO users (name, address) VALUES (%s, %s)", (name, address))
    conn.commit()
    print(f"[DB] Inserted User: {name}")
    cur.close()
    conn.close()

def generate_gps_stream():
    """Simulates a vehicle moving (Streaming Data)"""
    vehicle_id = random.choice(['V-101', 'V-102', 'V-103', 'V-104'])
    data = {
        'vehicle_id': vehicle_id,
        'latitude': float(fake.latitude()),
        'longitude': float(fake.longitude()),
        'speed': random.randint(0, 120), # km/h
        'timestamp': time.time()
    }
    # Send to Kafka
    producer.send(KAFKA_TOPIC, data)
    print(f"[KAFKA] Sent GPS: {vehicle_id} speed={data['speed']}")

# 4. MAIN LOOP
if __name__ == "__main__":
    print("Waiting for services to warm up...")
    time.sleep(10) # Give Docker a moment
    create_table()
    
    try:
        while True:
            # 50% chance to create a user, 100% chance to send GPS
            if random.random() > 0.5:
                generate_user()
            
            generate_gps_stream()
            
            # Sleep to simulate real-time (don't flood your CPU)
            time.sleep(1) 
    except KeyboardInterrupt:
        print("\nStopping Generator...")
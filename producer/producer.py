import os
import json
import time
import random
from confluent_kafka import Producer
from datetime import datetime

BOOTSTRAP = os.getenv('CONFLUENT_BOOTSTRAP_SERVERS')
API_KEY = os.getenv('CONFLUENT_API_KEY')
API_SECRET = os.getenv('CONFLUENT_API_SECRET')

conf = {
    'bootstrap.servers': BOOTSTRAP,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': API_KEY,
    'sasl.password': API_SECRET
}

p = Producer(conf)

NYC_COORDS = [
    (40.7128, -74.0060), # Manhattan
    (40.6782, -73.9442), # Brooklyn
    (40.7282, -73.7949), # Queens
    (40.8448, -73.8648), # Bronx
    (40.5795, -74.1502)  # Staten Island
]

ITEMS = ['couch', 'mattress', 'dresser', 'table', 'chair']


def drand_coords():
    base = random.choice(NYC_COORDS)
    # jitter ~ up to 0.02 degrees (~1-2km)
    return base[0] + random.uniform(-0.02, 0.02), base[1] + random.uniform(-0.02, 0.02)


def send_drop():
    lat, lon = drand_coords()
    ev = {
        'event_id': f'evt-{int(time.time()*1000)}-{random.randint(1,1000)}',
        'ts': datetime.utcnow().isoformat()+'Z',
        'lat': lat,
        'lon': lon,
        'item_type': random.choice(ITEMS)
    }
    p.produce('drops', json.dumps(ev).encode('utf-8'))
    p.flush()
    print('sent drop', ev)


def send_subscription(user_id='user-1', radius_miles=0.5):
    lat, lon = drand_coords()
    sub = {
        'user_id': user_id,
        'lat': lat,
        'lon': lon,
        'radius_miles': radius_miles,
        'ts': datetime.utcnow().isoformat()+'Z'
    }
    p.produce('subscriptions', json.dumps(sub).encode('utf-8'))
    p.flush()
    print('sent subscription', sub)


if __name__ == '__main__':
    # seed some subscriptions
    for i in range(10):
        send_subscription(user_id=f'user-{i+1}', radius_miles=0.5)
        time.sleep(0.2)

    # then emit drops continuously
    while True:
        send_drop()
        time.sleep(1)

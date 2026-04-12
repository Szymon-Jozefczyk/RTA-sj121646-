from kafka import KafkaConsumer
from collections import defaultdict, deque
import json
from datetime import datetime

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='velocity-detector-v2',
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

user_timestamps: dict[str, deque] = defaultdict(deque)

WINDOW_SECONDS = 60
MAX_TRANSACTIONS = 3

print(f"Alert: uzytkownik wykonuje >{MAX_TRANSACTIONS} transakcje w ciągu {WINDOW_SECONDS}s\n")

for message in consumer:
    tx = message.value

    user_id   = tx['user_id']
    tx_id     = tx['tx_id']
    amount    = tx['amount']
    store     = tx['store']
    timestamp = tx['timestamp']

    event_time = datetime.fromisoformat(timestamp).timestamp()

    timestamps = user_timestamps[user_id]

    while timestamps and (event_time - timestamps[0]) > WINDOW_SECONDS:
        timestamps.popleft()

    timestamps.append(event_time)

    tx_count = len(timestamps)

    if tx_count > MAX_TRANSACTIONS:
        print(
            f" VELOCITY ALERT | {user_id} | {tx_count} transakcji w {WINDOW_SECONDS}s "
            f"| Ostatnia: {tx_id} | {amount:.2f} PLN | {store} | {timestamp}"
        )
    else:
        print(f"   OK | {user_id} | {tx_count}/{MAX_TRANSACTIONS} transakcji w oknie | {tx_id}")

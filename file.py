from kafka import KafkaProducer
import json
import ccxt
from datetime import *
import random

#uses ccxt to get bitcoin price from binance
exchange = ccxt.binance()
ohlcv = exchange.fetch_ohlcv('BTCUSDT', limit=1)
btc_price = ohlcv[0][4]

#gets current time and a random ID 
now = datetime.now().isoformat()
id = random.randint(10000000,99999999)

#connecting to kafka producer
producer = KafkaProducer(
    bootstrap_servers=f"{'test-tebzzz-54d8.aivencloud.com'}:{23530}",
    security_protocol="SSL",
    ssl_cafile="ca.pem",
    ssl_certfile="service.cert",
    ssl_keyfile="service.key",
    value_serializer=lambda v: json.dumps(v).encode('ascii')
)

#send message to topic 
producer.send(
 'btcprice',
 value=
    {
    "TXN_ID " : str(id),
    "TIME": str(now),
    "BTC_PRICE": btc_price,
    }
)
producer.flush()

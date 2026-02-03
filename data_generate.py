# generate_trades.py
import csv, random, uuid
from datetime import datetime, timedelta

def generate(path, n_trades=1000000, start_dt=datetime(2025,1,1,9,0)):
    instruments = ['TCBT','TGBT','TRET','TSWE']
    with open(path,'w',newline='') as f:
        w = csv.writer(f)
        w.writerow(['timestamp','instrument','price','volume','trade_id'])
        for i in range(n_trades):
            inst = random.choice(instruments)
            seconds = random.randint(0, 60*60*24*5)
            ts = (start_dt + timedelta(
                seconds=seconds,
                milliseconds=random.randint(0,999))
            ).isoformat() + 'Z'
            price = round(random.gauss(100 if inst=='TCBT' else 20, 1.5), 4)
            volume = max(1, int(random.expovariate(1/100)))
            w.writerow([ts, inst, price, volume, str(uuid.uuid4())])

if __name__ == '__main__':
    generate('trades_big.csv')
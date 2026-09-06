"""Monitor de gatilho Spot + Telegram. Usa somente ticker Spot."""
import argparse,os,time
from pathlib import Path
import pandas as pd,requests
from dotenv import load_dotenv
from bybit_spot_common import get_ticker

BASE_DIR=Path(__file__).resolve().parent
load_dotenv(BASE_DIR/".env")
FILE=BASE_DIR/"monitor_gatilho_grid_spot.xlsx"
TOKEN=os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID=os.getenv("TELEGRAM_CHAT_ID")

def send(text):
    if not TOKEN or not CHAT_ID:
        print(text); return
    r=requests.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage",
      data={"chat_id":CHAT_ID,"text":text},timeout=20)
    r.raise_for_status()

def once():
    df=pd.read_excel(FILE); changed=False
    for i,r in df.iterrows():
        if not bool(r.get("ATIVO",True)) or bool(r.get("ALERTADO",False)): continue
        par=str(r["Par"]).strip().upper(); gat=float(r["GATILHO"])
        tol=float(r.get("TOLERANCIA_PCT",1.0) or 1.0)
        last=float(get_ticker(par).get("lastPrice") or 0)
        dist=abs(last/gat-1)*100 if gat else 999
        if dist<=tol:
            send(
              f"SPOT GRID | {par}\nPreço: {last}\nGatilho: {gat}\nDistância: {dist:.3f}%\n"
              f"Faixa: {r.get('LOWER','')} - {r.get('UPPER','')}\nGrids: {r.get('GRIDS','')}\n"
              f"TP: {r.get('TP','')} | SL: {r.get('SL','')}\n"
              f"TS retração equity: {r.get('TS_RETRACAO_PCT','')}%\nTrailing Up: {r.get('TRAILING_UP','')}"
            )
            df.at[i,"ALERTADO"]=True; changed=True
    if changed: df.to_excel(FILE,index=False)

if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--once",action="store_true")
    p.add_argument("--interval",type=int,default=60)
    a=p.parse_args()
    if a.once: once()
    else:
        while True:
            once(); time.sleep(max(60,a.interval))

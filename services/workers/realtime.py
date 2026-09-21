"""Executable Redis Streams realtime worker.

The worker persists normalized events through the selected Store. Model inference is
performed by adapters before publication; unavailable adapters must publish UNKNOWN.
"""
import argparse, os, time
from services.api.storage import Store
from services.realtime.stream import RedisStreamBus

def handle_once(bus, store, count=10):
    processed=0
    for message_id,event in bus.read(count=count):
        try:
            store.add_event(event)
            bus.ack(message_id)
            processed+=1
        except Exception:
            bus.retry_or_dead_letter(message_id,event)
    return processed

def main():
    parser=argparse.ArgumentParser(); parser.add_argument('--once',action='store_true'); args=parser.parse_args()
    bus=RedisStreamBus(); store=Store(); bus.ensure_group()
    if args.once: print({'processed':handle_once(bus,store)}); return
    print({'worker':'realtime','status':'ready','stream':bus.stream,'group':bus.group})
    while True: handle_once(bus,store); time.sleep(0.05)
if __name__=='__main__': main()

#!/usr/bin/python
# -*- coding: utf-8 -*-
import time
import random
import signal
import sys
import argparse
import curses
import mosquitto

parser = argparse.ArgumentParser(description="Heavy load MQTT publisher.")

parser.add_argument("--host", default='localhost',
                   help="mqtt host to connect to. Defaults to localhost.")

parser.add_argument("-p", "--port", type=int, default=1883,
                   help="network port to connect to. Defaults to 1883.")

parser.add_argument("-k", "--keepalive", type=int, default=60,
                   help="keep alive in seconds for this client. Defaults to 60.")

parser.add_argument("-r", "--root", default="monitor/test",
                   help="root topic to publish too. Defaults to 'monitor/test'")

parser.add_argument("--payload", default='{"value": 12345.7, "timestamp": 1366127221}',
                   help='payload to publish. Defaults to {"value": 12345.7, "timestamp": 1366127221}.')

parser.add_argument("-x", "--random_payload", type=bool, default=False,
                   help='create random payloads to publish.')

parser.add_argument("-s", "--sleep", type=int, default=0,
                   help="number of milliseconds to sleep between publish bursts")

# todo: add arguments

args = parser.parse_args()

flags = {
  "connected": False,
  "sent": 0
}

screen = curses.initscr()
curses.noecho()
curses.curs_set(0)
screen.keypad(1)
screen.timeout(1)

def signal_handler(signal, frame):
  curses.nocbreak()
  screen.keypad(0)
  curses.echo()
  curses.endwin()
  sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

def draw():
  screen.clear()
  screen.addstr(1,2, "Mosquitto Publisher")
  screen.addstr(2,2, "Connected: %s (%s:%d, %d)" % ( ("Yes" if flags["connected"] else "No"), args.host, args.port, args.keepalive ))
  screen.addstr(5,2, "Sent %d" % flags["sent"])
  screen.addstr(7,2, "Press 'q' to quit")

def on_connect(mosq, obj, rc):
  flags["connected"] = True
  draw()

def on_disconnect(mosq, obj, rc):
  flags["connected"] = False
  draw()

def on_publish(mosq, obj, mid):
  flags["sent"] += 1
  if flags["sent"] % 10 == 0: draw()

client = mosquitto.Mosquitto()

# Register mosquitto callbacks
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_publish = on_publish

client.connect(args.host, args.port, args.keepalive)
draw()

while True:
  flags["connected"] = True
  if args.random_payload:
    client.publish(args.root, "{'value': %s}" % random.randint(0,100))
  else:
    client.publish(args.root, args.payload)
  
  time.sleep(1)

  event = screen.getch()
  if event == ord("q"): break
  
curses.endwin()


#!/usr/bin/python
# -*- coding: utf-8 -*-

import signal
import sys
import argparse
import curses
import mosquitto
import datetime

parser = argparse.ArgumentParser(description='Monitors a mosquitto MQTT broker.')
parser.add_argument("--host", default='localhost',
                   help="mqtt host to connect to. Defaults to localhost.")
parser.add_argument("-p", "--port", type=int, default=1883,
                   help="network port to connect to. Defaults to 1883.")
parser.add_argument("-k", "--keepalive", type=int, default=60,
                   help="keep alive in seconds for this client. Defaults to 60.")

args = parser.parse_args()

"""

$SYS/broker/load/connections/+
The moving average of the number of CONNECT packets received by the broker over different time intervals. The final "+" of the hierarchy can be 1min, 5min or 15min. The value returned represents the number of connections received in 1 minute, averaged over 1, 5 or 15 minutes.

$SYS/broker/load/bytes/received/+
The moving average of the number of bytes received by the broker over different time intervals. The final "+" of the hierarchy can be 1min, 5min or 15min. The value returned represents the number of bytes received in 1 minute, averaged over 1, 5 or 15 minutes.

$SYS/broker/load/bytes/sent/+
The moving average of the number of bytes sent by the broker over different time intervals. The final "+" of the hierarchy can be 1min, 5min or 15min. The value returned represents the number of bytes sent in 1 minute, averaged over 1, 5 or 15 minutes.

$SYS/broker/load/messages/received/+
The moving average of the number of all types of MQTT messages received by the broker over different time intervals. The final "+" of the hierarchy can be 1min, 5min or 15min. The value returned represents the number of messages received in 1 minute, averaged over 1, 5 or 15 minutes.

$SYS/broker/load/messages/sent/+
The moving average of the number of all types of MQTT messages sent by the broker over different time intervals. The final "+" of the hierarchy can be 1min, 5min or 15min. The value returned represents the number of messages send in 1 minute, averaged over 1, 5 or 15 minutes.

$SYS/broker/load/publish/dropped/+
The moving average of the number of publish messages dropped by the broker over different time intervals. This shows the rate at which durable clients that are disconnected are losing messages. The final "+" of the hierarchy can be 1min, 5min or 15min. The value returned represents the number of messages dropped in 1 minute, averaged over 1, 5 or 15 minutes.

$SYS/broker/load/publish/received/+
The moving average of the number of publish messages received by the broker over different time intervals. The final "+" of the hierarchy can be 1min, 5min or 15min. The value returned represents the number of publish messages received in 1 minute, averaged over 1, 5 or 15 minutes.

$SYS/broker/load/publish/sent/+
The moving average of the number of publish messages sent by the broker over different time intervals. The final "+" of the hierarchy can be 1min, 5min or 15min. The value returned represents the number of publish messages sent in 1 minute, averaged over 1, 5 or 15 minutes.

$SYS/broker/load/sockets/+
The moving average of the number of socket connections opened to the broker over different time intervals. The final "+" of the hierarchy can be 1min, 5min or 15min. The value returned represents the number of socket connections in 1 minute, averaged over 1, 5 or 15 minutes.

$SYS/broker/messages/inflight
The number of messages with QoS>0 that are awaiting acknowledgments.

$SYS/broker/messages/stored
The number of messages currently held in the message store. This includes retained messages and messages queued for durable clients.

$SYS/broker/publish/messages/dropped
The total number of publish messages that have been dropped due to inflight/queuing limits. See the max_inflight_messages and max_queued_messages options in mosquitto.conf(5) for more information.

$SYS/broker/publish/messages/received
The total number of PUBLISH messages received since the broker started.

$SYS/broker/publish/messages/sent
The total number of PUBLISH messages sent since the broker started.

##################################3

"""

d = datetime.datetime.now()
f = open('file-%s.log' % d.strftime("%Y%m%d%H%M%S"), 'w')

# Broker Version
SYS_VERSION = "$SYS/broker/version"
# The amount of time in seconds the broker has been online.
SYS_UPTIME = "$SYS/broker/uptime"
# The total number of subscriptions active on the broker
SYS_SUBSCRIPTION_COUNT = "$SYS/broker/subscriptions/count"
# The number of currently connected clients.
SYS_CLIENTS_CONNECTED = "$SYS/broker/clients/connected"
# The number of disconnected persistent clients that have been expired and removed through the persistent_client_expiration option.
SYS_CLIENTS_EXPIRED = "$SYS/broker/clients/expired"
# The total number of persistent clients (with clean session disabled) that are registered at the broker but are currently disconnected.
SYS_CLIENTS_DISCONNETED = "$SYS/broker/clients/disconnected"
# The total number of active and inactive clients currently connected and registered on the broker.
SYS_CLIENTS_TOTAL = "$SYS/broker/clients/total"
# The maximum number of clients that have been connected to the broker at the same time.
SYS_MAX_CLIENTS_CONNECTED = "$SYS/broker/clients/maximum"

#### Totals ####
# The total number of bytes received since the broker started.
SYS_BYTES_RECEIVED = "$SYS/broker/bytes/received"
# The total number of bytes sent since the broker started.
SYS_BYTES_SENT = "$SYS/broker/bytes/sent"


SYS_MESSAGES_DROPPED = "$SYS/broker/messages/dropped"
SYS_MESSAGES_RECEIVED = "$SYS/broker/messages/received"
SYS_MESSAGES_SENT = "$SYS/broker/messages/sent"

# Average
SYS_LOAD_BYTES_RECEIVED = "$SYS/broker/load/bytes/received/1min"
SYS_LOAD_BYTES_SENT = "$SYS/broker/load/bytes/sent/1min"
SYS_LOAD_PUBLISHED_RECEIVED = "$SYS/broker/load/publish/received/1min"
SYS_LOAD_PUBLISHED_SENT = "$SYS/broker/load/publish/sent/1min"

topics = [
  SYS_VERSION,
  SYS_UPTIME,
  SYS_SUBSCRIPTION_COUNT,
  SYS_CLIENTS_CONNECTED,
  SYS_CLIENTS_DISCONNETED,
  SYS_CLIENTS_EXPIRED,
  SYS_CLIENTS_TOTAL,
  SYS_MAX_CLIENTS_CONNECTED,
  ########################
  SYS_BYTES_RECEIVED,
  SYS_BYTES_SENT,
  SYS_MESSAGES_DROPPED,
  SYS_MESSAGES_RECEIVED,
  SYS_MESSAGES_SENT,
  SYS_LOAD_BYTES_RECEIVED,
  SYS_LOAD_BYTES_SENT,
  SYS_LOAD_PUBLISHED_RECEIVED,
  SYS_LOAD_PUBLISHED_SENT
]

stats = {}

flags = {
  "connected": False
}

screen = curses.initscr()
curses.noecho()
curses.curs_set(0) 
screen.keypad(1)
screen.timeout(5)

def draw():
  
  receivedMb = float(stats.get(SYS_BYTES_RECEIVED, 0.0)) / 1024.0 / 1024.0
  sentMb = float(stats.get(SYS_BYTES_SENT, 0.0)) / 1024.0 / 1024.0
  
  receivedKbps = float(stats.get(SYS_LOAD_BYTES_RECEIVED, 0.0)) / 1024.0 / 60.0
  sentKbps = float(stats.get(SYS_LOAD_BYTES_SENT, 0.0)) / 1024.0 / 60.0
  
  screen.clear()
  screen.addstr(1,2, "Mosquitto Stats (%s, UPTIME=%s) %s" % (stats.get(SYS_VERSION), stats.get(SYS_UPTIME), datetime.datetime.now()))
  screen.addstr(2,2, "Connected: %s (%s:%d, %d)" % ( ("Yes" if flags["connected"] else "No"), args.host, args.port, args.keepalive ))
  screen.addstr(5,2, "         |  Received\t\tSent\t\tReceived/min\t\tSent/min")
  screen.addstr(6,2, "-------------------------------------------------------------------------------")
  screen.addstr(7,2, "Bytes    |  %.2f Mb\t\t%.2f Mb\t\t%.2f kbps\t\t%.2f kbps" % (receivedMb, sentMb, receivedKbps, sentKbps ))    
  screen.addstr(8,2, "Messages |  %s\t\t%s\t\t%s\t\t%s" % (stats.get(SYS_MESSAGES_RECEIVED), stats.get(SYS_MESSAGES_SENT), stats.get(SYS_LOAD_PUBLISHED_RECEIVED), stats.get(SYS_LOAD_PUBLISHED_SENT) ))    
  screen.addstr(11,2, "------------------------------------------------------------------------------")
  screen.addstr(12,2, "Messages dropped: %s" % stats.get(SYS_MESSAGES_DROPPED))
  screen.addstr(13,2, "Total Subscriptions: %s" % stats.get(SYS_SUBSCRIPTION_COUNT))
  screen.addstr(14,2, "Clients Disconnected: %s" % stats.get(SYS_CLIENTS_DISCONNETED))
  screen.addstr(15,2, "Clients Connected: %s" % stats.get(SYS_CLIENTS_CONNECTED))
  screen.addstr(16,2, "Clients Expired: %s" % stats.get(SYS_CLIENTS_EXPIRED))
  screen.addstr(17,2, "Clients Totals: %s" % stats.get(SYS_CLIENTS_TOTAL))


  screen.addstr(23,2, "Press 'q' to quit")

def signal_handler(signal, frame):
  curses.nocbreak()
  screen.keypad(0)
  curses.echo()
  curses.endwin()
  sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

def on_connect(mosq, obj, rc):
  flags["connected"] = True
  draw()
  for topic in topics:
    mosq.subscribe(topic, 0)
    
def on_disconnect(mosq, obj, rc):
  flags["connected"] = False
  draw()

def on_message(mosq, obj, msg):
  stats[msg.topic] = str(msg.payload)
  draw()

def on_log(mosq, obj, level, string):
  print(string)

mqttc = mosquitto.Mosquitto()

# Register callbacks
mqttc.on_message = on_message
mqttc.on_connect = on_connect
mqttc.on_disconnect = on_disconnect
#mqttc.on_log = on_log

# Connect on start
mqttc.connect(args.host, args.port, args.keepalive)

draw()

while True:
  
  rc = mqttc.loop()
  if rc != 0: break
  
  event = screen.getch()
  
  if event == ord("q"): break
  elif event == ord("c"): 
    mqttc.connect("127.0.0.1")
  elif event == ord("d"):
    mqttc.disconnect()
  
  draw()

curses.endwin()
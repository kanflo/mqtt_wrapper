#!/usr/bin/python
import paho.mqtt.client as mqtt
import time
import traceback
import logging

class bridge:

    def __init__(self, mqtt_topic, client_id = "bridge", user_id = "",password = "", host = "127.0.0.1", port = 1883, keepalive = 60):
        self.mqtt_topic = mqtt_topic
        self.client_id = client_id
        self.user_id = user_id
        self.password = password
        self.host = host
        self.port = port
        self.keepalive = keepalive

        self.disconnect_flag = False
        self.rc = 1
        self.timeout = 0

        self.client = mqtt.Client(self.client_id, clean_session=True)
        self.client.username_pw_set(self.user_id, self.password)

        self.client.on_log = self.on_log
        self.client.on_socket_close = self.on_socket_close
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message
        self.client.on_unsubscribe = self.on_unsubscribe
        self.client.on_subscribe = self.on_subscribe

        self.connect()

    def on_log(client, userdata, level, buf):
        logging.info("log: %s" % buf)

    def on_socket_close(userdata, sock):
        logging.error("Socket closed")

    def connect(self):
        while self.rc != 0:
            try:
                self.rc = self.client.connect(self.host, self.port, self.keepalive)
            except Exception as e:
                logging.error("Connection failed")
            time.sleep(2)
            self.timeout = self.timeout + 2

    def msg_process(self, msg):
        pass

    def looping(self, loop_timeout = .1):
        self.client.loop(loop_timeout)

    def on_connect(self, client, userdata, flags, rc):
        logging.info("Connected with result code "+str(rc))
        self.client.subscribe(self.mqtt_topic)
        self.timeout = 0

    def on_disconnect(self, client, userdata, rc):
        logging.error("Disconnected")
        if rc != 0:
            if not self.disconnect_flag:
                logging.error("Unexpected disconnect, reconnecting.")
                self.rc = rc
                self.connect()

    def on_message(self, client, userdata, msg):
        try:
            self.msg_process(msg)
        except Exception as e:
            logging.error("Caught exception", exc_info = True)

    def unsubscribe(self):
        logging.info("Unsubscribing")
        self.client.unsubscribe(self.mqtt_topic)

    def disconnect(self):
        logging.info("Disconnecting")
        self.disconnect_flag = True
        self.client.disconnect()

    def on_unsubscribe(self, client, userdata, mid):
        if (self.mqtt_topic == '#'):
            logging.info("Unsubscribed to all the topics" )
        else:
            logging.info("Unsubscribed to '%s'" % self.mqtt_topic)

    def on_subscribe(self, client, userdata, mid, granted_qos):
        if (self.mqtt_topic == '#'):
            logging.info("Subscribed to all the topics" )
        else:
            logging.info("Subscribed to '%s'" % self.mqtt_topic)

    def hook(self):
        self.unsubscribe()
        self.disconnect()
        logging.info("Shutting down")

    def get_timeout(self):
        return self.timeout

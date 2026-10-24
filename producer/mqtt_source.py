"""Subscription to the HSL MQTT broker over TLS, with automatic reconnects."""

from __future__ import annotations

import logging
import ssl
from collections.abc import Callable

import certifi
import paho.mqtt.client as mqtt

from producer.hfp import Envelope
from transit_platform.config import SourceSettings

logger = logging.getLogger(__name__)

RECONNECT_MIN_DELAY_S = 1
RECONNECT_MAX_DELAY_S = 60


class HfpSubscriber:
    """Delivers every message on the configured HFP topics to ``handle`` as an Envelope.

    paho runs the network loop in its own thread and reconnects with exponential backoff.
    Subscriptions are renewed in ``on_connect``, so they survive a reconnect.
    """

    def __init__(self, settings: SourceSettings, handle: Callable[[Envelope], None]) -> None:
        self.settings = settings
        self.handle = handle
        self.connected = False
        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv311, clean_session=True
        )
        self.client.tls_set(ca_certs=certifi.where(), tls_version=ssl.PROTOCOL_TLS_CLIENT)
        self.client.reconnect_delay_set(RECONNECT_MIN_DELAY_S, RECONNECT_MAX_DELAY_S)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message

    def start(self) -> None:
        logger.info(
            "event=mqtt_connecting host=%s port=%d",
            self.settings.mqtt_host,
            self.settings.mqtt_port,
        )
        self.client.connect_async(self.settings.mqtt_host, self.settings.mqtt_port, keepalive=30)
        self.client.loop_start()

    def stop(self) -> None:
        self.client.disconnect()
        self.client.loop_stop()
        logger.info("event=mqtt_stopped")

    def on_connect(self, client: mqtt.Client, _userdata, _flags, reason_code, _props) -> None:
        if reason_code.is_failure:
            logger.error("event=mqtt_connect_failed reason=%s", reason_code)
            return
        self.connected = True
        client.subscribe([(topic, 0) for topic in self.settings.mqtt_topics])
        logger.info("event=mqtt_connected topics=%s", ",".join(self.settings.mqtt_topics))

    def on_disconnect(self, _client, _userdata, _flags, reason_code, _props) -> None:
        self.connected = False
        level = logging.WARNING if reason_code.is_failure else logging.INFO
        logger.log(level, "event=mqtt_disconnected reason=%s", reason_code)

    def on_message(self, _client, _userdata, message: mqtt.MQTTMessage) -> None:
        try:
            self.handle(Envelope.from_mqtt(message.topic, message.payload))
        except Exception:
            logger.exception("event=message_handler_failed topic=%s", message.topic)

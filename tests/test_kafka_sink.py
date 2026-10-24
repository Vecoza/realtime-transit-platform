from producer.hfp import Envelope
from producer.kafka_sink import KafkaSink
from transit_platform.config import StreamSettings

TOPIC = "/hfp/v2/journey/ongoing/vp/bus/0018/01090/4611/1/X/21:49/1/5/60;24/1/2/3"


class FakeMessage:
    def key(self) -> bytes:
        return b"0018/01090"


class FakeProducer:
    def __init__(self, full_times: int = 0) -> None:
        self.full_times = full_times
        self.produced: list[dict] = []
        self.polls = 0

    def produce(self, topic: str, key: bytes | None, value: bytes, on_delivery) -> None:
        if self.full_times:
            self.full_times -= 1
            raise BufferError("queue full")
        self.produced.append({"topic": topic, "key": key, "value": value, "cb": on_delivery})

    def poll(self, _timeout: float) -> int:
        self.polls += 1
        return 0

    def flush(self, _timeout: float) -> int:
        for message in self.produced:
            message["cb"](None, FakeMessage())
        return 0


def sink_with(producer: FakeProducer) -> KafkaSink:
    return KafkaSink(StreamSettings(bootstrap_servers="unused:9092"), producer=producer)


def test_send_uses_the_vehicle_key_and_the_envelope_as_value() -> None:
    producer = FakeProducer()
    envelope = Envelope.from_mqtt(TOPIC, b'{"VP":{}}')

    sink_with(producer).send(envelope)

    (message,) = producer.produced
    assert message["topic"] == "hfp.raw"
    assert message["key"] == b"0018/01090"
    assert Envelope.from_json(message["value"]) == envelope


def test_send_waits_for_queue_space_instead_of_dropping() -> None:
    producer = FakeProducer(full_times=2)
    sink = sink_with(producer)

    sink.send(Envelope.from_mqtt(TOPIC, b"{}"))

    assert len(producer.produced) == 1
    assert producer.polls == 2
    assert sink.stats.sent == 1


def test_delivery_results_are_counted() -> None:
    producer = FakeProducer()
    sink = sink_with(producer)
    for _ in range(3):
        sink.send(Envelope.from_mqtt(TOPIC, b"{}"))

    assert sink.flush() == 0
    sink.on_delivery(object(), FakeMessage())

    assert (sink.stats.sent, sink.stats.delivered, sink.stats.failed) == (3, 3, 1)

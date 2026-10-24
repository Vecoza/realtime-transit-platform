import threading
from datetime import UTC, datetime, timedelta

from producer.hfp import Envelope
from producer.replay import replay

TOPIC = "/hfp/v2/journey/ongoing/vp/bus/0018/01090/4611/1/X/21:49/1/5/60;24/1/2/3"
START = datetime(2026, 10, 23, 19, 0, tzinfo=UTC)


def envelopes(offsets_s: list[float]) -> list[Envelope]:
    return [
        Envelope.from_mqtt(TOPIC, f'{{"n":{i}}}'.encode(), START + timedelta(seconds=offset))
        for i, offset in enumerate(offsets_s)
    ]


class FakeTime:
    """A clock that only moves when the replayer waits."""

    def __init__(self) -> None:
        self.now = 100.0
        self.waits: list[float] = []

    def clock(self) -> float:
        return self.now

    def stop_event(self) -> threading.Event:
        fake = self

        class Stop(threading.Event):
            def wait(self, timeout: float | None = None) -> bool:
                fake.waits.append(round(timeout, 6))
                fake.now += timeout
                return self.is_set()

        return Stop()


def test_replay_keeps_recorded_spacing_scaled_by_speed() -> None:
    fake = FakeTime()
    sent: list[tuple[float, str]] = []

    result = replay(
        envelopes([0, 1, 1, 3.5]),
        lambda e: sent.append((fake.now, e.payload)),
        speed=10,
        stop=fake.stop_event(),
        clock=fake.clock,
    )

    assert result.published == 4
    assert fake.waits == [0.1, 0.25]
    assert [round(at - 100.0, 6) for at, _ in sent] == [0.0, 0.1, 0.1, 0.35]
    assert [payload for _, payload in sent] == ['{"n":0}', '{"n":1}', '{"n":2}', '{"n":3}']


def test_replay_at_speed_zero_never_waits() -> None:
    fake = FakeTime()
    sent: list[Envelope] = []

    result = replay(envelopes([0, 60, 120]), sent.append, 0, fake.stop_event(), fake.clock)

    assert result.published == 3
    assert fake.waits == []


def test_replay_stops_when_asked() -> None:
    fake = FakeTime()
    stop = fake.stop_event()
    sent: list[Envelope] = []

    def send_and_stop(envelope: Envelope) -> None:
        sent.append(envelope)
        stop.set()

    result = replay(envelopes([0, 0, 0]), send_and_stop, 0, stop, fake.clock)

    assert result.published == 1

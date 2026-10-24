from transit_platform.config import DatabaseSettings, SourceSettings, StreamSettings


def test_database_url_is_built_from_env(monkeypatch):
    monkeypatch.setenv("POSTGRES_USER", "u")
    monkeypatch.setenv("POSTGRES_PASSWORD", "p")
    monkeypatch.setenv("POSTGRES_HOST", "db")
    monkeypatch.setenv("POSTGRES_PORT", "6543")
    monkeypatch.setenv("POSTGRES_DB", "t")

    assert DatabaseSettings.from_env().url == "postgresql+psycopg://u:p@db:6543/t"


def test_stream_settings_read_bootstrap_servers(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9092")
    assert StreamSettings.from_env().bootstrap_servers == "broker:9092"


def test_source_port_is_integer(monkeypatch):
    monkeypatch.setenv("HSL_MQTT_PORT", "1883")
    assert SourceSettings.from_env().mqtt_port == 1883


def test_source_subscribes_to_ongoing_journeys_of_each_event_type(monkeypatch):
    monkeypatch.setenv("HFP_EVENT_TYPES", "vp, arr,,dep")
    assert SourceSettings.from_env().mqtt_topics == [
        "/hfp/v2/journey/ongoing/vp/#",
        "/hfp/v2/journey/ongoing/arr/#",
        "/hfp/v2/journey/ongoing/dep/#",
    ]


def test_stream_settings_default_to_the_raw_topic(monkeypatch):
    monkeypatch.delenv("HFP_RAW_TOPIC", raising=False)
    settings = StreamSettings.from_env()
    assert (settings.raw_topic, settings.raw_partitions) == ("hfp.raw", 6)

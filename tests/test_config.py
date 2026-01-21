import yaml

from sempervigil.config import load_config


def test_load_config_allows_missing_sources(tmp_path):
    config_path = tmp_path / "config.yml"
    config_path.write_text(yaml.safe_dump({"app": {"name": "Test"}}))

    config = load_config(str(config_path))
    assert config.sources == []


def test_load_config_accepts_sources_if_present(tmp_path):
    config_path = tmp_path / "config.yml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "sources": [
                    {"id": "s1", "type": "rss", "url": "https://example.com/feed"}
                ]
            }
        )
    )

    config = load_config(str(config_path))
    assert config.sources[0].id == "s1"

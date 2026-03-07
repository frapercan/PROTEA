from pathlib import Path

from protea.infrastructure.settings import load_settings


def test_load_settings_reads_yaml(tmp_path: Path):
    (tmp_path / "protea" / "config").mkdir(parents=True, exist_ok=True)
    (tmp_path / "protea" / "config" / "system.yaml").write_text(
        "database:\n  url: postgresql+psycopg://u:p@localhost:5432/x\nqueue:\n  amqp_url: amqp://a:b@localhost:5672/\n",
        encoding="utf-8",
    )

    s = load_settings(tmp_path)
    assert s.db_url.endswith("/x")
    assert s.amqp_url.startswith("amqp://a:b@")

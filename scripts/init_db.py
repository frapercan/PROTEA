from pathlib import Path

# IMPORTANT: import models so Base.metadata is populated
import protea.infrastructure.orm.models  # noqa: F401
from protea.infrastructure.database.engine import create_engine
from protea.infrastructure.orm.base import Base
from protea.infrastructure.settings import load_settings

if __name__ == "__main__":
    project_root = Path(__file__).resolve().parents[1]
    settings = load_settings(project_root)

    engine = create_engine(settings.db_url)
    Base.metadata.create_all(engine)

    print("DB initialized (tables created).")

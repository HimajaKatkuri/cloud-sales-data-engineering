"""Reset the LOCAL environment: drop the pipeline's schemas and clear data zones.

    python scripts/reset_local.py --yes

Refuses to run unless ENVIRONMENT is local or ci, so it cannot wipe a production database
by accident.
"""

from __future__ import annotations

import argparse
import shutil
import sys

from sqlalchemy import text

from sales_pipeline.config.settings import get_settings
from sales_pipeline.loading.db import create_db_engine

ZONES = ("raw", "processed", "rejected")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--yes", action="store_true", help="confirm the reset")
    parser.add_argument("--keep-source", action="store_true", help="keep data/source files")
    args = parser.parse_args()

    settings = get_settings()
    if settings.environment not in ("local", "ci"):
        print(f"Refusing to reset: ENVIRONMENT={settings.environment}", file=sys.stderr)
        return 2
    if not args.yes:
        print("Add --yes to drop schemas core, analytics, pipeline and clear data zones.")
        return 1

    with create_db_engine(settings).begin() as conn:
        for schema in ("analytics", "core", "pipeline"):
            conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
    zones = list(ZONES) if args.keep_source else [*ZONES, "source"]
    for zone in zones:
        path = settings.local_data_dir / zone
        if path.exists():
            for child in path.iterdir():
                if child.name != ".gitkeep":
                    shutil.rmtree(child) if child.is_dir() else child.unlink()
    print(f"Dropped schemas and cleared data zones: {', '.join(zones)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

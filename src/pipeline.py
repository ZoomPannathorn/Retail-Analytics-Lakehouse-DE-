from __future__ import annotations

from src.extract.generate_raw_data import main as generate_raw_data
from src.load.load_to_postgres import run as load_to_postgres
from src.quality.checks import run_checks
from src.transform.build_silver import run as build_silver


def main() -> None:
    generate_raw_data()
    build_silver()
    run_checks()
    load_to_postgres()


if __name__ == "__main__":
    main()


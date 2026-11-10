"""Keep unused SQL drivers out of the complete dependency lockfile."""

from pathlib import Path
import tomllib
import unittest


class PostgresOnlyDependenciesTests(unittest.TestCase):
    def test_lockfile_excludes_other_sql_drivers_and_their_native_libraries(self):
        root = Path(__file__).resolve().parents[2]
        packages = tomllib.loads((root / "Cargo.lock").read_text())["package"]
        names = {package["name"] for package in packages}
        self.assertIn("sqlx-postgres", names)
        self.assertFalse(
            names & {"sqlx", "sqlx-sqlite", "sqlx-mysql", "libsqlite3-sys"},
            "Use the PostgreSQL components instead of SQLx's multi-driver facade",
        )

import sqlite3
from pathlib import Path


class DatabaseManager:

    def __init__(self):

        project_root = Path(__file__).resolve().parents[2]

        db_path = project_root / "database" / "data_engine.db"

        db_path.parent.mkdir(parents=True, exist_ok=True)

        self.conn = sqlite3.connect(db_path)

        self.create_table()

    def create_table(self):

        cursor = self.conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS quotes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            text TEXT,
            author TEXT,
            tags TEXT
        )
        """)

        self.conn.commit()

    def insert_quotes(self, df):

        df.to_sql("quotes", self.conn, if_exists="append", index=False)
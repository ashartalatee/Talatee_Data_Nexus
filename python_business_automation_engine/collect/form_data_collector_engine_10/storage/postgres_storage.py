"""
╔══════════════════════════════════════════════════════════════════════════════╗
║          FORM DATA COLLECTOR ENGINE #10 — PostgreSQL Storage                ║
║          Python Business Automation Engine | Portfolio Professional          ║
╚══════════════════════════════════════════════════════════════════════════════╝

Module  : storage/postgres_storage.py
Purpose : Save and query form data in PostgreSQL database.

          Features:
          1. Insert DataFrame → PostgreSQL table (append / replace / upsert)
          2. Upsert — update existing rows, insert new ones (no duplicates)
          3. Auto-create table from DataFrame schema
          4. Chunked insert for large datasets (avoid memory/timeout issues)
          5. Connection pooling with retry logic
          6. Table health check & row count
          7. Query back → DataFrame
          8. Graceful degradation — if Postgres unavailable, log & skip
          9. DSN from .env (POSTGRES_DSN)

Install:
    pip install sqlalchemy psycopg2-binary

DSN format:
    postgresql://username:password@host:port/database
    Example: postgresql://admin:secret@localhost:5432/form_data_db

Author  : Python Automation Engine
Version : 1.0.0
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, Optional

import pandas as pd
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

# ─────────────────────────────────────────────────────────────────────────────
# OPTIONAL IMPORTS — graceful degradation if not installed
# ─────────────────────────────────────────────────────────────────────────────
try:
    import sqlalchemy as sa
    from sqlalchemy import text
    from sqlalchemy.exc import SQLAlchemyError
    SA_AVAILABLE = True
except ImportError:
    SA_AVAILABLE = False
    logger.warning(
        "⚠️  sqlalchemy not installed. PostgreSQL storage disabled.\n"
        "   Install with: pip install sqlalchemy psycopg2-binary"
    )

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────
DEFAULT_DSN       = os.getenv("POSTGRES_DSN", "")
DEFAULT_TABLE     = os.getenv("POSTGRES_TABLE", "form_responses")
DEFAULT_SCHEMA    = os.getenv("POSTGRES_SCHEMA", "public")
CHUNK_SIZE        = int(os.getenv("POSTGRES_CHUNK_SIZE", 500))
MAX_RETRIES       = int(os.getenv("POSTGRES_MAX_RETRIES", 3))
RETRY_DELAY       = float(os.getenv("POSTGRES_RETRY_DELAY", 2.0))

IfExists = Literal["append", "replace", "fail"]


# ─────────────────────────────────────────────────────────────────────────────
# RESULT
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class PostgresStorageResult:
    """
    Result of a PostgreSQL storage operation.

    Attributes:
        table        : Target table name.
        rows_written : Rows inserted or updated.
        mode         : "append", "replace", "upsert", or "query".
        duration_s   : Seconds taken.
        success      : True if no error.
        error        : Error message if success=False.
    """
    table       : str
    rows_written: int
    mode        : str
    duration_s  : float = 0.0
    success     : bool  = True
    error       : str   = ""

    @property
    def summary(self) -> dict:
        return {
            "table"       : self.table,
            "rows_written": self.rows_written,
            "mode"        : self.mode,
            "duration_s"  : round(self.duration_s, 3),
            "success"     : self.success,
            "error"       : self.error or None,
        }

    def __repr__(self) -> str:
        return (
            f"<PostgresStorageResult table='{self.table}' "
            f"rows={self.rows_written} mode={self.mode} "
            f"success={self.success} duration={self.duration_s:.2f}s>"
        )


# ─────────────────────────────────────────────────────────────────────────────
# CORE POSTGRES STORAGE
# ─────────────────────────────────────────────────────────────────────────────

class PostgresStorage:
    """
    Production-grade PostgreSQL storage handler.

    Args:
        dsn          : PostgreSQL DSN string.
                       Default from env: POSTGRES_DSN
        table        : Target table name. Default from env: POSTGRES_TABLE
        schema       : PostgreSQL schema. Default "public".
        chunk_size   : Rows per INSERT batch. Default 500.
        upsert_key   : Column(s) used as unique key for upsert mode.
                       e.g. "email" or ["email", "timestamp"]
    """

    def __init__(
        self,
        dsn       : str            = DEFAULT_DSN,
        table     : str            = DEFAULT_TABLE,
        schema    : str            = DEFAULT_SCHEMA,
        chunk_size: int            = CHUNK_SIZE,
        upsert_key: Optional[str | list[str]] = None,
    ) -> None:
        self.dsn        = dsn
        self.table      = table
        self.schema     = schema
        self.chunk_size = chunk_size
        self.upsert_key = [upsert_key] if isinstance(upsert_key, str) else (upsert_key or [])
        self._engine    = None   # lazy init

        if not SA_AVAILABLE:
            logger.error("sqlalchemy not installed — PostgresStorage disabled.")

    # ── private: engine ───────────────────────────────────────────────────────

    def _get_engine(self):
        """Lazy-create and cache SQLAlchemy engine."""
        if self._engine is not None:
            return self._engine

        if not self.dsn:
            raise ValueError(
                "POSTGRES_DSN not set.\n"
                "Add to .env: POSTGRES_DSN=postgresql://user:pass@host:5432/dbname"
            )

        logger.info(f"🔌 Connecting to PostgreSQL | table='{self.schema}.{self.table}'")
        self._engine = sa.create_engine(
            self.dsn,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,          # auto-reconnect on stale connections
            pool_recycle=3600,           # recycle connections every hour
            connect_args={"connect_timeout": 10},
        )
        return self._engine

    def _qualified_table(self) -> str:
        """Return schema.table string."""
        return f"{self.schema}.{self.table}"

    # ── private: clean for postgres ───────────────────────────────────────────

    def _prepare_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare DataFrame for PostgreSQL insertion:
        - Replace empty strings with None (→ NULL)
        - Sanitize column names (lowercase, replace spaces with _)
        - Drop metadata columns that shouldn't go to DB
        """
        df = df.copy()

        # clean column names
        df.columns = [
            col.lower()
               .strip()
               .replace(" ", "_")
               .replace("-", "_")
               .replace(".", "_")
            for col in df.columns
        ]

        # empty string → None (→ NULL in Postgres)
        df.replace("", None, inplace=True)

        # add ingested_at timestamp
        df["_ingested_at"] = datetime.now(tz=None).isoformat()

        return df

    # ── public: ping ─────────────────────────────────────────────────────────

    def ping(self) -> bool:
        """Check if database connection is alive. Returns True/False."""
        if not SA_AVAILABLE:
            return False
        try:
            engine = self._get_engine()
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("✅ PostgreSQL connection OK")
            return True
        except Exception as exc:
            logger.error(f"❌ PostgreSQL ping failed: {exc}")
            return False

    # ── public: insert ────────────────────────────────────────────────────────

    def insert(
        self,
        df       : pd.DataFrame,
        if_exists: IfExists = "append",
    ) -> PostgresStorageResult:
        """
        Insert DataFrame into PostgreSQL table.

        Args:
            df        : DataFrame to insert.
            if_exists : What to do if table exists:
                        "append"  — add rows to existing table.
                        "replace" — drop and recreate table (⚠️ destructive!).
                        "fail"    — raise error if table exists.

        Returns:
            PostgresStorageResult
        """
        if not SA_AVAILABLE:
            return PostgresStorageResult(
                table=self.table, rows_written=0, mode=if_exists,
                success=False, error="sqlalchemy not installed",
            )

        if df.empty:
            logger.warning("⚠️  PostgresStorage.insert() called with empty DataFrame.")
            return PostgresStorageResult(
                table=self.table, rows_written=0, mode=if_exists, success=True
            )

        start  = time.perf_counter()
        df_pg  = self._prepare_df(df)
        total  = 0

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                engine = self._get_engine()

                # chunked insert
                for chunk_start in range(0, len(df_pg), self.chunk_size):
                    chunk = df_pg.iloc[chunk_start: chunk_start + self.chunk_size]
                    chunk.to_sql(
                        self.table,
                        engine,
                        schema   =self.schema,
                        if_exists=if_exists if chunk_start == 0 else "append",
                        index    =False,
                        method   ="multi",
                        chunksize=self.chunk_size,
                    )
                    total += len(chunk)
                    logger.debug(
                        f"  📤 Inserted chunk {chunk_start}–{chunk_start+len(chunk)} "
                        f"→ {self._qualified_table()}"
                    )

                duration = time.perf_counter() - start
                logger.info(
                    f"💾 PostgreSQL insert → {self._qualified_table()} "
                    f"({total} rows | {duration:.2f}s | mode={if_exists})"
                )
                return PostgresStorageResult(
                    table=self.table, rows_written=total,
                    mode=if_exists, duration_s=duration, success=True,
                )

            except SQLAlchemyError as exc:
                if attempt < MAX_RETRIES:
                    wait = RETRY_DELAY * attempt
                    logger.warning(f"⚠️  Insert failed (attempt {attempt}) — retry in {wait}s: {exc}")
                    time.sleep(wait)
                    self._engine = None   # reset engine on failure
                else:
                    duration = time.perf_counter() - start
                    logger.error(f"❌ PostgreSQL insert failed after {MAX_RETRIES} attempts: {exc}")
                    return PostgresStorageResult(
                        table=self.table, rows_written=total,
                        mode=if_exists, duration_s=duration,
                        success=False, error=str(exc),
                    )

        return PostgresStorageResult(
            table=self.table, rows_written=0, mode=if_exists, success=False
        )

    # ── public: upsert ────────────────────────────────────────────────────────

    def upsert(self, df: pd.DataFrame) -> PostgresStorageResult:
        """
        Upsert — INSERT new rows, UPDATE existing rows (based on upsert_key).

        Requires self.upsert_key to be set.
        Uses PostgreSQL ON CONFLICT DO UPDATE syntax.

        Args:
            df : DataFrame to upsert.

        Returns:
            PostgresStorageResult
        """
        if not SA_AVAILABLE:
            return PostgresStorageResult(
                table=self.table, rows_written=0, mode="upsert",
                success=False, error="sqlalchemy not installed",
            )

        if not self.upsert_key:
            logger.warning("⚠️  upsert_key not set — falling back to insert(append).")
            return self.insert(df, if_exists="append")

        if df.empty:
            logger.warning("⚠️  PostgresStorage.upsert() called with empty DataFrame.")
            return PostgresStorageResult(
                table=self.table, rows_written=0, mode="upsert", success=True
            )

        start = time.perf_counter()
        df_pg = self._prepare_df(df)

        # clean upsert keys to match prepared column names
        upsert_keys_clean = [
            k.lower().strip().replace(" ", "_")
            for k in self.upsert_key
        ]

        total = 0
        try:
            engine = self._get_engine()

            # build ON CONFLICT upsert SQL
            cols         = list(df_pg.columns)
            conflict_cols= ", ".join(upsert_keys_clean)
            update_cols  = [c for c in cols if c not in upsert_keys_clean]
            update_set   = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
            placeholders = ", ".join(f":{c}" for c in cols)
            col_names    = ", ".join(cols)

            sql = text(f"""
                INSERT INTO {self._qualified_table()} ({col_names})
                VALUES ({placeholders})
                ON CONFLICT ({conflict_cols})
                DO UPDATE SET {update_set}
            """)

            with engine.begin() as conn:
                for chunk_start in range(0, len(df_pg), self.chunk_size):
                    chunk  = df_pg.iloc[chunk_start: chunk_start + self.chunk_size]
                    records = chunk.to_dict(orient="records")
                    conn.execute(sql, records)
                    total += len(chunk)

            duration = time.perf_counter() - start
            logger.info(
                f"💾 PostgreSQL upsert → {self._qualified_table()} "
                f"({total} rows | key={upsert_keys_clean} | {duration:.2f}s)"
            )
            return PostgresStorageResult(
                table=self.table, rows_written=total,
                mode="upsert", duration_s=duration, success=True,
            )

        except Exception as exc:
            duration = time.perf_counter() - start
            logger.error(f"❌ PostgreSQL upsert failed: {exc}")
            return PostgresStorageResult(
                table=self.table, rows_written=0,
                mode="upsert", duration_s=duration,
                success=False, error=str(exc),
            )

    # ── public: query ─────────────────────────────────────────────────────────

    def query(
        self,
        sql   : Optional[str] = None,
        limit : int           = 1000,
        where : Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Query data from the table into a DataFrame.

        Args:
            sql   : Custom SQL query. If None, selects all from table.
            limit : Max rows to return. Default 1000.
            where : Optional WHERE clause (without the WHERE keyword).
                    e.g. "kota = 'Surabaya' AND total > 1000000"

        Returns:
            DataFrame (empty on error).

        Examples:
            # All rows
            df = storage.query()

            # Custom SQL
            df = storage.query("SELECT * FROM form_responses WHERE tier = 'Gold'")

            # Using where shorthand
            df = storage.query(where="kota = 'Jakarta'", limit=500)
        """
        if not SA_AVAILABLE:
            logger.error("sqlalchemy not installed")
            return pd.DataFrame()

        try:
            engine = self._get_engine()
            if sql is None:
                where_clause = f"WHERE {where}" if where else ""
                sql = (
                    f"SELECT * FROM {self._qualified_table()} "
                    f"{where_clause} LIMIT {limit}"
                )

            df = pd.read_sql(text(sql), engine)
            logger.info(
                f"📂 PostgreSQL query → {self._qualified_table()} "
                f"({len(df)} rows)"
            )
            return df

        except Exception as exc:
            logger.error(f"❌ PostgreSQL query failed: {exc}")
            return pd.DataFrame()

    # ── public: table info ────────────────────────────────────────────────────

    def table_info(self) -> dict:
        """Return metadata about the target table."""
        if not SA_AVAILABLE:
            return {"error": "sqlalchemy not installed"}

        try:
            engine = self._get_engine()
            with engine.connect() as conn:
                # row count
                count_sql = text(
                    f"SELECT COUNT(*) FROM {self._qualified_table()}"
                )
                row_count = conn.execute(count_sql).scalar()

                # column info
                col_sql = text(f"""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = :schema AND table_name = :table
                    ORDER BY ordinal_position
                """)
                cols = conn.execute(
                    col_sql, {"schema": self.schema, "table": self.table}
                ).fetchall()

            return {
                "table"     : self._qualified_table(),
                "row_count" : row_count,
                "columns"   : [{"name": c[0], "type": c[1]} for c in cols],
            }

        except Exception as exc:
            return {"error": str(exc), "table": self._qualified_table()}

    # ── public: delete ────────────────────────────────────────────────────────

    def delete_where(self, condition: str) -> bool:
        """
        Delete rows matching a WHERE condition.

        Args:
            condition : SQL WHERE condition (without WHERE keyword).
                        e.g. "_ingested_at < '2024-01-01'"

        Returns:
            True if successful, False otherwise.
        """
        if not SA_AVAILABLE:
            return False
        try:
            engine = self._get_engine()
            with engine.begin() as conn:
                sql = text(
                    f"DELETE FROM {self._qualified_table()} WHERE {condition}"
                )
                result = conn.execute(sql)
                logger.info(
                    f"🗑️  Deleted {result.rowcount} rows "
                    f"from {self._qualified_table()} WHERE {condition}"
                )
            return True
        except Exception as exc:
            logger.error(f"❌ Delete failed: {exc}")
            return False


# ─────────────────────────────────────────────────────────────────────────────
# CONVENIENCE FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def save_postgres(
    df        : pd.DataFrame,
    table     : str            = DEFAULT_TABLE,
    dsn       : str            = DEFAULT_DSN,
    mode      : IfExists       = "append",
    upsert_key: Optional[str | list[str]] = None,
) -> PostgresStorageResult:
    """
    One-liner PostgreSQL save. Used by main.py and data_router handlers.

    Args:
        df         : DataFrame to save.
        table      : Target table name.
        dsn        : PostgreSQL connection DSN.
        mode       : "append", "replace", or "upsert".
        upsert_key : Column for upsert deduplication.

    Example:
        from storage.postgres_storage import save_postgres
        save_postgres(df, table="form_responses", mode="upsert", upsert_key="email")
    """
    storage = PostgresStorage(dsn=dsn, table=table, upsert_key=upsert_key)
    if mode == "upsert":
        return storage.upsert(df)
    return storage.insert(df, if_exists=mode)


# ─────────────────────────────────────────────────────────────────────────────
# CLI TEST  (requires live PostgreSQL connection)
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 65)
    print("  PostgreSQL Storage — Connection Test")
    print("=" * 65)

    dsn = os.getenv("POSTGRES_DSN", "")

    if not dsn:
        print(
            "\n⚠️  POSTGRES_DSN not set in .env\n"
            "   Set it to test live connection:\n"
            "   POSTGRES_DSN=postgresql://user:pass@localhost:5432/dbname\n\n"
            "   Skipping live test — showing dry-run mode instead."
        )

        # ── dry-run simulation ────────────────────────────────────────────────
        sample = pd.DataFrame({
            "email" : ["budi@gmail.com", "siti@yahoo.com"],
            "nama"  : ["Budi Santoso", "Siti Rahayu"],
            "kota"  : ["Surabaya", "Jakarta"],
            "total" : [1_500_000, 2_500_000],
        })

        print("\n  📋 Sample DataFrame that would be inserted:")
        print(sample.to_string(index=False))
        print(
            f"\n  Target  : public.form_responses\n"
            f"  Mode    : append\n"
            f"  Rows    : {len(sample)}\n"
            f"  Columns : {list(sample.columns)}\n"
            f"\n  ✅ PostgresStorage class loaded OK — ready when DSN is configured."
        )

    else:
        # ── live test ─────────────────────────────────────────────────────────
        storage = PostgresStorage(dsn=dsn, table="test_form_data", upsert_key="email")

        print("\n  TEST 1: ping()")
        ok = storage.ping()
        print(f"  Connected: {ok}")

        if ok:
            sample = pd.DataFrame({
                "email" : ["budi@gmail.com", "siti@yahoo.com", "andi@outlook.com"],
                "nama"  : ["Budi Santoso", "Siti Rahayu", "Andi Wijaya"],
                "kota"  : ["Surabaya", "Jakarta", "Bandung"],
                "total" : [1_500_000, 2_500_000, 750_000],
            })

            print("\n  TEST 2: insert(append)")
            r1 = storage.insert(sample, if_exists="replace")   # replace for clean test
            print(f"  Result: {r1}")

            print("\n  TEST 3: upsert()")
            sample_update = sample.copy()
            sample_update.loc[0, "total"] = 9_999_999  # update Budi's total
            r2 = storage.upsert(sample_update)
            print(f"  Result: {r2}")

            print("\n  TEST 4: query()")
            df_back = storage.query(limit=10)
            print(f"  Rows returned: {len(df_back)}")
            print(df_back[["email", "nama", "total"]].to_string(index=False))

            print("\n  TEST 5: table_info()")
            info = storage.table_info()
            print(f"  Row count: {info.get('row_count')}")
            for col in info.get("columns", []):
                print(f"    {col['name']:<25}: {col['type']}")

            print("\n  TEST 6: delete_where()")
            storage.delete_where("email = 'andi@outlook.com'")

    print("\n✅ PostgreSQL storage test complete!")
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║          FORM DATA COLLECTOR ENGINE #10 — Data Router                       ║
║          Python Business Automation Engine | Portfolio Professional          ║
╚══════════════════════════════════════════════════════════════════════════════╝

Module  : processors/data_router.py
Purpose : Route processed rows to different storage destinations
          based on configurable conditions.

          Use cases:
          - Route "Gold" tier customers → premium_leads.csv
          - Route Jakarta orders → storage Jakarta warehouse DB
          - Route invalid rows → quarantine sheet
          - Route new registrants → email campaign queue
          - Split by date range, category, source, score, etc.

          Features:
          1. Rule-based routing (condition → destination)
          2. Priority-ordered rules (first match wins)
          3. Default/fallback destination
          4. Multi-destination routing (one row → multiple targets)
          5. Route statistics & audit log
          6. Dry-run mode (preview routing without writing)

Author  : Python Automation Engine
Version : 1.0.0
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Optional

import pandas as pd
from loguru import logger


# ─────────────────────────────────────────────────────────────────────────────
# ROUTING RULE
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class RoutingRule:
    """
    A single routing rule.

    Args:
        name        : Human-readable rule name (used in logs & reports).
        destination : Target destination key (e.g. "premium_csv", "postgres", "quarantine").
        condition   : Callable(row: pd.Series) → bool.
                      Row is routed here if condition returns True.
        priority    : Lower number = evaluated first. Default 100.
        exclusive   : If True, matched row won't be checked against further rules.
                      If False, row can match multiple rules (multi-destination). Default True.
        description : Optional human-readable description for documentation.

    Examples:
        RoutingRule(
            name="Gold tier",
            destination="premium_leads",
            condition=lambda row: float(row.get("total", 0)) >= 2_000_000,
            priority=10,
        )

        RoutingRule(
            name="Jakarta customers",
            destination="jakarta_db",
            condition=lambda row: str(row.get("kota", "")).lower() == "jakarta",
            priority=20,
            exclusive=False,   # also send to default
        )

        RoutingRule(
            name="Missing email",
            destination="quarantine",
            condition=lambda row: not str(row.get("email", "")).strip(),
            priority=1,        # check first!
        )
    """
    name       : str
    destination: str
    condition  : Callable[[pd.Series], bool]
    priority   : int  = 100
    exclusive  : bool = True
    description: str  = ""

    def matches(self, row: pd.Series) -> bool:
        """Safely evaluate condition against a row."""
        try:
            return bool(self.condition(row))
        except Exception as exc:
            logger.warning(f"⚠️  Rule '{self.name}' condition error: {exc}")
            return False


# ─────────────────────────────────────────────────────────────────────────────
# ROUTING RESULT
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class RoutingResult:
    """
    Result of routing a DataFrame through rules.

    Attributes:
        routes      : Dict mapping destination → DataFrame.
        row_log     : Per-row audit log (index, destination, rule matched).
        stats       : Per-destination row counts.
        total_rows  : Total input rows.
        unrouted    : Rows that matched no rule and have no default destination.
    """
    routes    : dict[str, pd.DataFrame]
    row_log   : pd.DataFrame
    stats     : dict[str, int]
    total_rows: int
    unrouted  : pd.DataFrame

    @property
    def destinations(self) -> list[str]:
        return list(self.routes.keys())

    @property
    def summary(self) -> dict:
        return {
            "total_rows"   : self.total_rows,
            "destinations" : self.destinations,
            "stats"        : self.stats,
            "unrouted"     : len(self.unrouted),
        }

    def get(self, destination: str) -> pd.DataFrame:
        """Get routed DataFrame for a destination. Returns empty DF if not found."""
        return self.routes.get(destination, pd.DataFrame())

    def __repr__(self) -> str:
        return (
            f"<RoutingResult total={self.total_rows} "
            f"destinations={self.destinations} "
            f"unrouted={len(self.unrouted)}>"
        )


# ─────────────────────────────────────────────────────────────────────────────
# CORE ROUTER
# ─────────────────────────────────────────────────────────────────────────────

class DataRouter:
    """
    Route rows to different destinations based on configurable rules.

    Args:
        rules               : List of RoutingRule objects.
        default_destination : Destination for rows that match no rule.
                              None = unrouted rows go to result.unrouted.
        dry_run             : If True, compute routing but don't call any
                              storage functions. Default False.
        add_route_col       : Add '_destination' column to each routed df.
                              Default True.
    """

    def __init__(
        self,
        rules               : list[RoutingRule],
        default_destination : Optional[str] = "default",
        dry_run             : bool          = False,
        add_route_col       : bool          = True,
    ) -> None:
        self.rules               = sorted(rules, key=lambda r: r.priority)
        self.default_destination = default_destination
        self.dry_run             = dry_run
        self.add_route_col       = add_route_col

        self._validate_rules()

    def _validate_rules(self) -> None:
        names = [r.name for r in self.rules]
        dupes = [n for n in names if names.count(n) > 1]
        if dupes:
            logger.warning(f"⚠️  Duplicate rule names: {list(set(dupes))}")
        logger.debug(
            f"DataRouter initialized | {len(self.rules)} rules | "
            f"default='{self.default_destination}'"
        )

    # ── private ───────────────────────────────────────────────────────────────

    def _route_row(
        self, row: pd.Series
    ) -> list[str]:
        """
        Evaluate all rules against one row.
        Returns list of matched destination keys.
        """
        matched: list[str] = []

        for rule in self.rules:
            if rule.matches(row):
                matched.append(rule.destination)
                if rule.exclusive:
                    return matched   # stop at first exclusive match

        if not matched and self.default_destination:
            matched.append(self.default_destination)

        return matched

    # ── public: route ─────────────────────────────────────────────────────────

    def route(self, df: pd.DataFrame) -> RoutingResult:
        """
        Route all rows in df according to registered rules.

        Args:
            df : Transformed DataFrame from DataTransformer.

        Returns:
            RoutingResult with per-destination DataFrames and audit log.
        """
        if df.empty:
            logger.warning("⚠️  Empty DataFrame passed to DataRouter.")
            return RoutingResult(
                routes={}, row_log=pd.DataFrame(),
                stats={}, total_rows=0, unrouted=pd.DataFrame(),
            )

        logger.info(
            f"🔀 Routing {len(df)} rows | "
            f"{len(self.rules)} rules | "
            f"default='{self.default_destination}'"
            f"{' | DRY RUN' if self.dry_run else ''}"
        )

        # ── route each row ────────────────────────────────────────────────────
        buckets : dict[str, list[int]] = {}  # destination → list of row indices
        log_rows: list[dict]           = []

        for idx, row in df.iterrows():
            destinations = self._route_row(row)

            for dest in destinations:
                buckets.setdefault(dest, []).append(idx)

            log_rows.append({
                "row_index"   : idx,
                "destinations": ", ".join(destinations) if destinations else "UNROUTED",
                "rules_matched": len(destinations),
            })

        # ── build per-destination DataFrames ──────────────────────────────────
        routes: dict[str, pd.DataFrame] = {}
        stats : dict[str, int]          = {}

        for dest, indices in buckets.items():
            dest_df = df.loc[indices].copy().reset_index(drop=True)
            if self.add_route_col:
                dest_df["_destination"] = dest
            routes[dest] = dest_df
            stats[dest]  = len(dest_df)
            logger.info(f"  📦 Destination '{dest}': {len(dest_df)} row(s)")

        # ── unrouted ──────────────────────────────────────────────────────────
        all_routed = {idx for indices in buckets.values() for idx in indices}
        unrouted   = df.loc[~df.index.isin(all_routed)].copy()
        if not unrouted.empty:
            logger.warning(f"⚠️  {len(unrouted)} unrouted row(s) — no rule matched.")

        row_log = pd.DataFrame(log_rows)

        logger.info(
            f"✅ Routing complete | "
            f"{len(routes)} destination(s) | "
            f"{len(unrouted)} unrouted"
        )

        return RoutingResult(
            routes    = routes,
            row_log   = row_log,
            stats     = stats,
            total_rows= len(df),
            unrouted  = unrouted,
        )

    # ── public: route + dispatch ──────────────────────────────────────────────

    def route_and_dispatch(
        self,
        df        : pd.DataFrame,
        handlers  : dict[str, Callable[[pd.DataFrame], Any]],
    ) -> dict[str, Any]:
        """
        Route rows then call registered handler functions per destination.

        Args:
            df       : DataFrame to route.
            handlers : Dict mapping destination → callable(df) → Any.
                       Handler receives the routed DataFrame for that destination.

        Returns:
            Dict mapping destination → handler return value.

        Example:
            from storage.csv_storage import save_csv
            from storage.postgres_storage import save_postgres

            handler_results = router.route_and_dispatch(df, {
                "premium_leads" : lambda d: save_csv(d, "outputs/premium.csv"),
                "quarantine"    : lambda d: save_csv(d, "outputs/quarantine.csv"),
                "default"       : lambda d: save_postgres(d, "form_responses"),
            })
        """
        result = self.route(df)

        dispatch_results: dict[str, Any] = {}

        for dest, dest_df in result.routes.items():
            if dest_df.empty:
                continue
            handler = handlers.get(dest)
            if handler is None:
                logger.warning(f"⚠️  No handler registered for destination '{dest}'")
                continue
            if self.dry_run:
                logger.info(f"🧪 DRY RUN — would dispatch '{dest}' ({len(dest_df)} rows)")
                dispatch_results[dest] = f"dry_run:{len(dest_df)}_rows"
                continue
            try:
                dispatch_results[dest] = handler(dest_df)
                logger.info(f"  ✅ Dispatched '{dest}' → {len(dest_df)} rows")
            except Exception as exc:
                logger.error(f"  ❌ Handler '{dest}' failed: {exc}")
                dispatch_results[dest] = f"error:{exc}"

        return dispatch_results

    # ── public: preview ───────────────────────────────────────────────────────

    def preview(self, df: pd.DataFrame, n: int = 5) -> None:
        """
        Print routing preview without dispatching.
        Shows which destination each row would go to.

        Args:
            df : DataFrame to preview.
            n  : Max rows to show per destination. Default 5.
        """
        result = self.route(df)
        print("\n" + "─" * 60)
        print(f"  🔀 ROUTING PREVIEW  |  {len(df)} rows  |  {len(self.rules)} rules")
        print("─" * 60)
        for dest, dest_df in result.routes.items():
            print(f"\n  📦 '{dest}' → {len(dest_df)} row(s)")
            show_cols = [c for c in dest_df.columns if not c.startswith("_")][:5]
            print(dest_df[show_cols].head(n).to_string(index=False))
        if not result.unrouted.empty:
            print(f"\n  ⚠️  UNROUTED → {len(result.unrouted)} row(s)")
        print("─" * 60 + "\n")


# ─────────────────────────────────────────────────────────────────────────────
# PRESET RULE FACTORIES
# ─────────────────────────────────────────────────────────────────────────────

class RouterPresets:
    """
    Ready-to-use routing rule presets for common form scenarios.
    Customize as needed for your project.
    """

    @staticmethod
    def by_tier(
        total_col   : str = "total",
        gold_min    : float = 2_000_000,
        silver_min  : float = 1_000_000,
    ) -> list[RoutingRule]:
        """Route by purchase total into Gold / Silver / Bronze destinations."""
        return [
            RoutingRule(
                name="Gold tier",
                destination="gold",
                condition=lambda row, c=total_col, m=gold_min:
                    _safe_float(row.get(c, 0)) >= m,
                priority=10,
            ),
            RoutingRule(
                name="Silver tier",
                destination="silver",
                condition=lambda row, c=total_col, m=silver_min:
                    _safe_float(row.get(c, 0)) >= m,
                priority=20,
            ),
            RoutingRule(
                name="Bronze tier",
                destination="bronze",
                condition=lambda row: True,
                priority=99,
            ),
        ]

    @staticmethod
    def by_city(
        city_col    : str,
        city_map    : dict[str, str],
        default     : str = "other_city",
    ) -> list[RoutingRule]:
        """
        Route rows by city name.

        Args:
            city_col : Column name containing city.
            city_map : {"Surabaya": "surabaya_queue", "Jakarta": "jakarta_queue"}
            default  : Destination for cities not in city_map.

        Example:
            RouterPresets.by_city(
                city_col="kota",
                city_map={"Surabaya": "sbq_db", "Jakarta": "jkt_db"},
            )
        """
        rules = []
        for i, (city, dest) in enumerate(city_map.items()):
            rules.append(RoutingRule(
                name=f"City: {city}",
                destination=dest,
                condition=lambda row, c=city_col, ci=city:
                    str(row.get(c, "")).strip().lower() == ci.lower(),
                priority=10 + i,
            ))
        rules.append(RoutingRule(
            name="Other city (default)",
            destination=default,
            condition=lambda row: True,
            priority=999,
        ))
        return rules

    @staticmethod
    def quarantine_invalid(
        required_cols: list[str],
    ) -> list[RoutingRule]:
        """
        Route rows with any missing required field to 'quarantine'.
        Valid rows go to 'valid'.
        """
        def has_missing(row: pd.Series) -> bool:
            for col in required_cols:
                val = row.get(col, "")
                if not str(val).strip() or str(val).lower() in ("nan", "none"):
                    return True
            return False

        return [
            RoutingRule(
                name="Quarantine invalid",
                destination="quarantine",
                condition=has_missing,
                priority=1,
            ),
            RoutingRule(
                name="Valid rows",
                destination="valid",
                condition=lambda row: True,
                priority=999,
            ),
        ]

    @staticmethod
    def by_source(
        source_col  : str = "_source",
        source_map  : Optional[dict[str, str]] = None,
    ) -> list[RoutingRule]:
        """Route rows based on their data source."""
        source_map = source_map or {
            "google_form"   : "google_form_storage",
            "csv_import"    : "csv_storage",
            "webhook"       : "webhook_storage",
        }
        rules = []
        for i, (src, dest) in enumerate(source_map.items()):
            rules.append(RoutingRule(
                name=f"Source: {src}",
                destination=dest,
                condition=lambda row, c=source_col, s=src:
                    str(row.get(c, "")).strip().lower() == s.lower(),
                priority=10 + i,
            ))
        return rules


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _safe_float(value: Any) -> float:
    """Safe float conversion, returns 0.0 on failure."""
    try:
        return float(str(value).replace(",", "").replace(".", "")
                     if isinstance(value, str) else value)
    except (ValueError, TypeError):
        return 0.0


# ─────────────────────────────────────────────────────────────────────────────
# CLI TEST
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 65)
    print("  Data Router — Quick Test")
    print("=" * 65)

    sample = pd.DataFrame({
        "nama"  : ["Budi", "Siti", "Andi", "Rina", "Dodi"],
        "email" : ["b@g.com", "", "a@g.com", "r@g.com", "d@g.com"],
        "kota"  : ["Surabaya", "Jakarta", "Surabaya", "Bandung", "Jakarta"],
        "total" : [3_000_000, 1_500_000, 500_000, 2_500_000, 0],
        "_source": ["google_form"] * 5,
    })

    print("\n📋 Input DataFrame:")
    print(sample.to_string(index=False))

    # ── Test 1: Quarantine + tier routing ─────────────────────────────────────
    print("\n" + "─" * 65)
    print("  TEST 1: Quarantine invalid → then route by tier")

    rules = [
        RoutingRule(
            name      = "Quarantine: missing email",
            destination="quarantine",
            condition = lambda row: not str(row.get("email", "")).strip(),
            priority  = 1,
        ),
        RoutingRule(
            name      = "Gold tier (≥ 2jt)",
            destination="gold",
            condition = lambda row: _safe_float(row.get("total", 0)) >= 2_000_000,
            priority  = 10,
        ),
        RoutingRule(
            name      = "Silver tier (≥ 1jt)",
            destination="silver",
            condition = lambda row: _safe_float(row.get("total", 0)) >= 1_000_000,
            priority  = 20,
        ),
        RoutingRule(
            name      = "Bronze (default)",
            destination="bronze",
            condition = lambda row: True,
            priority  = 99,
        ),
    ]

    router = DataRouter(rules, default_destination=None)
    result = router.route(sample)

    print(f"\n  Result: {result}")
    print(f"\n  Summary:")
    for k, v in result.summary.items():
        print(f"    {k:<18}: {v}")

    for dest, df_dest in result.routes.items():
        print(f"\n  📦 '{dest}' ({len(df_dest)} rows):")
        print(df_dest[["nama", "total", "email"]].to_string(index=False))

    # ── Test 2: Preset — by_tier ──────────────────────────────────────────────
    print("\n" + "─" * 65)
    print("  TEST 2: RouterPresets.by_tier()")

    router2 = DataRouter(RouterPresets.by_tier(total_col="total"))
    router2.preview(sample)

    # ── Test 3: Preset — quarantine_invalid ───────────────────────────────────
    print("  TEST 3: RouterPresets.quarantine_invalid()")

    router3 = DataRouter(
        RouterPresets.quarantine_invalid(required_cols=["nama", "email"]),
        default_destination=None,
    )
    result3 = router3.route(sample)
    print(f"  Valid rows    : {len(result3.get('valid'))}")
    print(f"  Quarantine    : {len(result3.get('quarantine'))}")

    # ── Test 4: route_and_dispatch ────────────────────────────────────────────
    print("\n" + "─" * 65)
    print("  TEST 4: route_and_dispatch() — DRY RUN")

    router4 = DataRouter(rules, dry_run=True)
    dispatched = router4.route_and_dispatch(sample, handlers={
        "gold"      : lambda df: print(f"    → Gold handler: {len(df)} rows"),
        "silver"    : lambda df: print(f"    → Silver handler: {len(df)} rows"),
        "bronze"    : lambda df: print(f"    → Bronze handler: {len(df)} rows"),
        "quarantine": lambda df: print(f"    → Quarantine handler: {len(df)} rows"),
    })
    print(f"\n  Dispatch results: {dispatched}")

    print("\n✅ All router tests completed!")
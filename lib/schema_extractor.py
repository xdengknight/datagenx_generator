"""Schema, statistics, and plan extraction adapters for database systems."""

import json
import os
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import mysql.connector
from mysql.connector import Error


@dataclass(frozen=True)
class HistogramBucket:
    """Database-neutral optimizer histogram bucket.

    The optional bounds are optimizer metadata and must be treated as source
    statistics, not as values to emit into generated synthetic data.
    """

    ordinal: int
    frequency: float
    cumulative_frequency: float
    num_distinct: Optional[int] = None
    lower_bound: Optional[Any] = None
    upper_bound: Optional[Any] = None


@dataclass(frozen=True)
class TopNEntry:
    """Database-neutral TopN/MCV entry.

    `ordinal` is the stable synthetic identity used by DataGenX. Adapter
    implementations intentionally do not expose source literal values here.
    """

    ordinal: int
    frequency: float
    count: Optional[int] = None


@dataclass(frozen=True)
class ColumnOptimizerStats:
    """Optimizer-visible statistics for one column, normalized across engines."""

    database_type: str
    table: str
    column: str
    row_count: Optional[int] = None
    ndv: Optional[int] = None
    histogram_type: Optional[str] = None
    histogram_buckets: Optional[List[HistogramBucket]] = None
    topn: Optional[List[TopNEntry]] = None


class StatisticsExtractor(ABC):
    """Abstract statistics and plan-inspection API for database engines."""

    @abstractmethod
    def analyze_table(self, table):
        """Run engine-appropriate ANALYZE to update optimizer statistics."""
        pass

    @abstractmethod
    def get_column_histogram(self, table, column):
        """
        Get a column histogram in DataGenX's internal format:
        {
            "histogram-type": "equi-height" | "singleton",
            "buckets": [[lower, upper, cumulative_freq, num_distinct], ...]
        }
        """
        pass

    @abstractmethod
    def get_column_cardinalities(self, table):
        """Return optimizer cardinality estimates keyed by column name."""
        pass

    def get_column_topn(self, table, column):
        """Return TopN/MCV entries as source-value-free optimizer stats.

        Engines with explicit TopN/MCV catalogs should override this. The
        default maps singleton histograms to TopN-like entries because a
        singleton histogram stores one bucket per frequent/distinct value.
        """
        histogram = self.get_column_histogram(table, column)
        return _topn_entries_from_histogram(histogram)

    def get_column_optimizer_stats(self, table, column):
        """Return normalized optimizer statistics for one column."""
        histogram = self.get_column_histogram(table, column)
        histogram_buckets = _histogram_buckets_from_histogram(histogram)
        cardinalities = self.get_column_cardinalities(table)
        ndv = cardinalities.get(column)
        if ndv is None and histogram_buckets:
            ndv = sum(
                bucket.num_distinct or 1
                for bucket in histogram_buckets
            )
        return ColumnOptimizerStats(
            database_type=self.__class__.__name__.replace("Extractor", "").lower(),
            table=table,
            column=column,
            row_count=self.get_table_cardinality(table).get("row_count"),
            ndv=ndv,
            histogram_type=(histogram or {}).get("histogram-type"),
            histogram_buckets=histogram_buckets,
            topn=self.get_column_topn(table, column),
        )

    def get_table_optimizer_stats(self, table):
        """Return normalized optimizer statistics for all columns in a table."""
        return {
            column: self.get_column_optimizer_stats(table, column)
            for column in self.get_columns(table)
        }

    @abstractmethod
    def get_index_cardinality(self, table):
        """Return optimizer cardinality estimates keyed by index and column."""
        pass

    @abstractmethod
    def get_table_cardinality(self, table):
        """Return row, column, and index cardinality metadata for a table."""
        pass

    @abstractmethod
    def get_explain_plan(self, query, analyze=False):
        """Return an engine-normalized EXPLAIN result for a query."""
        pass


class SchemaExtractor(StatisticsExtractor):
    """Abstract base class for schema, statistics, and plan extraction."""

    def __init__(self, host, user, password, database, port=None):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.conn = None
        self.cursor = None

    def connect(self):
        """Establish database connection."""
        try:
            self.conn = mysql.connector.connect(**self.connection_kwargs())
            self.cursor = self.conn.cursor()
            print(f"✅ Connected to {self.database} at {self.host}")
            return True
        except Error as e:
            print(f"❌ Connection failed: {e}")
            return False

    def connection_kwargs(self, **overrides):
        """Return mysql.connector connection keyword arguments."""
        return self.__class__.build_connection_kwargs(
            self.host,
            self.user,
            self.password,
            self.database,
            self.port,
            **overrides,
        )

    @classmethod
    def build_connection_kwargs(cls, host, user, password, database, port=None, **overrides):
        """Build mysql.connector connection keyword arguments."""
        kwargs = {
            "host": host,
            "port": int(port or 3306),
            "user": user,
            "password": password,
            "database": database,
        }
        kwargs.update(overrides)
        return kwargs

    def close(self):
        """Close database connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn and self.conn.is_connected():
            self.conn.close()

    def get_tables(self):
        """Get all tables in the database."""
        self.cursor.execute("""
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'
        """, (self.database,))
        return [t[0] for t in self.cursor.fetchall()]

    def get_columns(self, table):
        """Get all columns for a table with their types."""
        self.cursor.execute("""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
        """, (self.database, table))
        return {col: dtype.lower() for col, dtype in self.cursor.fetchall()}

    def get_primary_keys(self, table):
        """Get primary key columns for a table."""
        self.cursor.execute("""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = %s
              AND TABLE_NAME = %s
              AND CONSTRAINT_NAME = 'PRIMARY'
        """, (self.database, table))
        return {r[0] for r in self.cursor.fetchall()}

    def get_foreign_keys(self, table):
        """Get foreign key mappings: column -> (referenced_table, referenced_column)."""
        self.cursor.execute("""
            SELECT COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = %s
              AND TABLE_NAME = %s
              AND REFERENCED_TABLE_NAME IS NOT NULL
        """, (self.database, table))
        return {
            col: (ref_table, ref_col)
            for col, ref_table, ref_col in self.cursor.fetchall()
        }

    def get_table_ddl(self, table):
        """Get CREATE TABLE statement."""
        self.cursor.execute(f"SHOW CREATE TABLE `{self.database}`.`{table}`")
        return self.cursor.fetchone()[1]

    def get_table_dependencies(self):
        """Get foreign key dependencies: {table: [tables it depends on]}."""
        self.cursor.execute("""
            SELECT TABLE_NAME, REFERENCED_TABLE_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = %s
              AND REFERENCED_TABLE_NAME IS NOT NULL
        """, (self.database,))

        dependencies = {}
        for table, referenced_table in self.cursor.fetchall():
            if table not in dependencies:
                dependencies[table] = set()
            if referenced_table and referenced_table != table:
                dependencies[table].add(referenced_table)

        return {k: list(v) for k, v in dependencies.items()}

    def get_table_row_count(self, table):
        """Return exact row count for a table."""
        self.cursor.execute(f"SELECT COUNT(*) FROM `{self.database}`.`{table}`")
        return self.cursor.fetchone()[0]

    def get_distinct_count(self, table, column):
        """Return exact distinct count for one column."""
        self.cursor.execute(
            f"SELECT COUNT(DISTINCT `{column}`) FROM `{self.database}`.`{table}`"
        )
        return self.cursor.fetchone()[0]

    def get_table_cardinality(self, table):
        """Return row, column, and index cardinality metadata for a table."""
        return {
            "row_count": self.get_table_row_count(table),
            "columns": self.get_column_cardinalities(table),
            "indexes": self.get_index_cardinality(table),
        }

    def get_table_histograms(self, table):
        """Return all available column histograms for a table."""
        histograms = {}
        for column in self.get_columns(table):
            histogram = self.get_column_histogram(table, column)
            if histogram:
                histograms[column] = histogram
        return histograms

    def get_explain_plan(self, query, analyze=False):
        """Run row-format EXPLAIN and return columns plus rows.

        Engines with structured explain formats can override this method while
        callers use the same API.
        """
        prefix = "EXPLAIN ANALYZE" if analyze else "EXPLAIN"
        self.cursor.execute(f"{prefix} {query}")
        return {
            "format": "rows",
            "analyze": analyze,
            "columns": list(self.cursor.column_names),
            "rows": self.cursor.fetchall(),
        }


class MySQLExtractor(SchemaExtractor):
    """Schema extractor for MySQL databases."""

    def analyze_table(self, table):
        """Run ANALYZE TABLE and update histograms for numeric columns."""
        self.cursor.execute(f"ANALYZE TABLE `{self.database}`.`{table}`")
        self.cursor.fetchall()

        # Get numeric columns
        self.cursor.execute("""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
              AND DATA_TYPE IN (
                  'tinyint','smallint','mediumint','int','bigint',
                  'decimal','numeric','float','double'
              )
        """, (self.database, table))

        cols = [c[0] for c in self.cursor.fetchall()]
        if not cols:
            return

        # Update histograms
        self.cursor.execute(
            f"""
            ANALYZE TABLE `{self.database}`.`{table}`
            UPDATE HISTOGRAM ON {','.join(f'`{c}`' for c in cols)}
            WITH 100 BUCKETS
            """
        )
        self.cursor.fetchall()

    def get_column_histogram(self, table, column):
        """Get histogram from MySQL's column_statistics."""
        try:
            self.cursor.execute("""
                SELECT HISTOGRAM
                FROM information_schema.column_statistics
                WHERE SCHEMA_NAME = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s
            """, (self.database, table, column))

            result = self.cursor.fetchone()
            if result and result[0]:
                return json.loads(result[0])
            return None
        except Exception as e:
            # column_statistics might not exist in older MySQL versions
            return None

    def get_column_cardinalities(self, table):
        """Return broad MySQL column cardinalities when available.

        MySQL's portable metadata gives index cardinality, not full table-wide
        per-column NDV. Keep this empty so callers do not mistake index stats
        for exact column distributions; use get_index_cardinality() for SHOW
        INDEX-style estimates.
        """
        return {}

    def get_index_cardinality(self, table):
        """Return MySQL index cardinality estimates by index and column."""
        try:
            self.cursor.execute("""
                SELECT INDEX_NAME, COLUMN_NAME, SEQ_IN_INDEX, CARDINALITY, NON_UNIQUE
                FROM INFORMATION_SCHEMA.STATISTICS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                ORDER BY INDEX_NAME, SEQ_IN_INDEX
            """, (self.database, table))
            return _index_rows_to_cardinality(self.cursor.fetchall())
        except Exception:
            return {}


class SingleStoreExtractor(SchemaExtractor):
    """Schema extractor for SingleStore databases."""

    @classmethod
    def build_connection_kwargs(cls, host, user, password, database, port=None, **overrides):
        kwargs = super().build_connection_kwargs(
            host,
            user,
            password,
            database,
            port,
            auth_plugin="mysql_native_password",
        )
        kwargs.update(overrides)
        return kwargs

    def connect(self):
        """Establish database connection with SingleStore-compatible auth."""
        try:
            self.conn = mysql.connector.connect(**self.connection_kwargs())
            self.cursor = self.conn.cursor()
            print(f"Connected to {self.database} at {self.host}")
            return True
        except Error as e:
            print(f"Connection failed: {e}")
            return False

    def get_primary_keys(self, table):
        """Get primary key columns for a SingleStore table.

        SingleStore uses UNIQUE KEY `pk` (UNENFORCED RELY) instead of PRIMARY KEY.
        Falls back to parsing DDL if information_schema doesn't have it.
        """
        # Try standard PRIMARY first
        result = super().get_primary_keys(table)
        if result:
            return result

        # SingleStore: try UNIQUE KEY named 'pk'
        self.cursor.execute("""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = %s
              AND TABLE_NAME = %s
              AND CONSTRAINT_NAME = 'pk'
        """, (self.database, table))
        result = {r[0] for r in self.cursor.fetchall()}
        if result:
            return result

        # Last resort: parse DDL for UNIQUE KEY `pk` or PRIMARY KEY
        ddl = self.get_table_ddl(table)
        for pattern in [
            r"PRIMARY\s+KEY\s*\(([^)]+)\)",
            r"UNIQUE\s+KEY\s+`pk`\s*\(([^)]+)\)",
        ]:
            m = re.search(pattern, ddl, re.IGNORECASE)
            if m:
                return {c.strip().strip('`') for c in m.group(1).split(',')}

        return set()

    def analyze_table(self, table):
        """Run ANALYZE TABLE to collect statistics and create histograms.

        Creates histograms on numeric/date columns (excludes PK and string),
        then removes histograms from high-cardinality equi-height columns where
        comparison would be meaningless. Aligns with MySQL behavior.
        """
        # Step 1: Create histograms on numeric/date columns, exclude PK
        self.cursor.execute("""
            SELECT c.COLUMN_NAME
            FROM information_schema.columns c
            WHERE c.TABLE_SCHEMA = %s AND c.TABLE_NAME = %s
              AND c.DATA_TYPE IN ('int', 'bigint', 'smallint', 'tinyint', 'mediumint',
                                  'decimal', 'numeric', 'float', 'double',
                                  'date', 'datetime', 'timestamp', 'time', 'year')
              AND c.COLUMN_NAME NOT IN (
                  SELECT k.COLUMN_NAME
                  FROM information_schema.KEY_COLUMN_USAGE k
                  WHERE k.TABLE_SCHEMA = %s AND k.TABLE_NAME = %s
                    AND k.CONSTRAINT_NAME = 'PRIMARY'
              )
            ORDER BY c.ORDINAL_POSITION
        """, (self.database, table, self.database, table))
        numeric_cols = [row[0] for row in self.cursor.fetchall()]
        pk_cols = self.get_primary_keys(table)
        numeric_cols = [c for c in numeric_cols if c not in pk_cols]

        if numeric_cols:
            col_list = ", ".join(numeric_cols)
            self.cursor.execute(
                f"ANALYZE TABLE `{self.database}`.`{table}` COLUMNS {col_list} ENABLE"
            )
            self.cursor.fetchall()
        else:
            self.cursor.execute(f"ANALYZE TABLE `{self.database}`.`{table}`")
            self.cursor.fetchall()
            return

        # Step 2: Remove histograms from high-cardinality equi-height + PK columns
        try:
            self.cursor.execute("""
                SELECT COLUMN_NAME, MAX(UNIQUE_COUNT) as max_unique,
                       COUNT(*) as num_buckets
                FROM information_schema.ADVANCED_HISTOGRAMS
                WHERE DATABASE_NAME = %s AND TABLE_NAME = %s
                  AND BUCKET_INDEX >= 0 AND CARDINALITY > 0
                GROUP BY COLUMN_NAME
            """, (self.database, table))
            cols_to_disable = []
            for col, max_unique, num_buckets in self.cursor.fetchall():
                if max_unique > 1 and num_buckets > 20:
                    cols_to_disable.append(col)
                elif col in pk_cols:
                    cols_to_disable.append(col)
            if cols_to_disable:
                self.cursor.execute(
                    f"ALTER TABLE `{self.database}`.`{table}` AUTOSTATS_HISTOGRAM_MODE=OFF"
                )
                self.cursor.fetchall()
                for col in cols_to_disable:
                    self.cursor.execute(
                        f"ANALYZE TABLE `{self.database}`.`{table}` COLUMNS {col} DISABLE"
                    )
                    self.cursor.fetchall()
                self.cursor.execute(
                    f"ALTER TABLE `{self.database}`.`{table}` AUTOSTATS_HISTOGRAM_MODE=CREATE"
                )
                self.cursor.fetchall()
        except Exception:
            pass

    def get_column_histogram(self, table, column):
        """Get histogram from SingleStore's ADVANCED_HISTOGRAMS view."""
        try:
            # Try ADVANCED_HISTOGRAMS first (SingleStore 8.0+)
            self.cursor.execute("""
                SELECT BUCKET_INDEX, RANGE_MIN, RANGE_MAX,
                       CARDINALITY, UNIQUE_COUNT
                FROM information_schema.ADVANCED_HISTOGRAMS
                WHERE DATABASE_NAME = %s
                  AND TABLE_NAME = %s
                  AND COLUMN_NAME = %s
                  AND BUCKET_INDEX >= 0
                ORDER BY BUCKET_INDEX
            """, (self.database, table, column))

            buckets = self.cursor.fetchall()
            if buckets:
                return self._convert_singlestore_histogram(buckets)

        except Exception as e:
            # ADVANCED_HISTOGRAMS might not be available
            # Try alternative: COLUMN_STATISTICS
            try:
                self.cursor.execute("""
                    SELECT HISTOGRAM
                    FROM information_schema.COLUMN_STATISTICS
                    WHERE TABLE_SCHEMA = %s
                      AND TABLE_NAME = %s
                      AND COLUMN_NAME = %s
                """, (self.database, table, column))

                result = self.cursor.fetchone()
                if result and result[0]:
                    # If it's JSON format (similar to MySQL)
                    if isinstance(result[0], str):
                        return json.loads(result[0])
                    return result[0]
            except:
                pass

        return None

    def get_column_cardinalities(self, table):
        """Return SingleStore optimizer cardinality estimates keyed by column."""
        try:
            self.cursor.execute("""
                SELECT column_name, cardinality
                FROM information_schema.optimizer_statistics
                WHERE database_name = %s AND table_name = %s
            """, (self.database, table))
            return {
                col: int(cardinality)
                for col, cardinality in self.cursor.fetchall()
                if cardinality is not None
            }
        except Exception:
            return {}

    def get_index_cardinality(self, table):
        """Return SingleStore index cardinality estimates by index and column."""
        try:
            self.cursor.execute("""
                SELECT INDEX_NAME, COLUMN_NAME, SEQ_IN_INDEX, CARDINALITY, NON_UNIQUE
                FROM INFORMATION_SCHEMA.STATISTICS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                ORDER BY INDEX_NAME, SEQ_IN_INDEX
            """, (self.database, table))
            return _index_rows_to_cardinality(self.cursor.fetchall())
        except Exception:
            return {}

    def _convert_singlestore_histogram(self, buckets):
        """
        Convert SingleStore ADVANCED_HISTOGRAMS buckets to standard format.
        Input: [(bucket_index, range_min, range_max, cardinality, unique_count), ...]
        Output: {"histogram-type": "equi-height", "buckets": [...]}
        """
        if not buckets:
            return None

        # Filter out invalid and zero-cardinality buckets
        valid_buckets = [
            b for b in buckets
            if b[1] is not None and b[2] is not None and b[3] is not None
            and b[3] > 0
        ]

        if not valid_buckets:
            return None

        histogram = {
            "histogram-type": "equi-height",
            "buckets": []
        }

        cumulative_freq = 0.0
        total_freq = sum(row[3] for row in valid_buckets)

        if total_freq == 0:
            return None

        # Detect singleton-like histograms: all buckets have unique_count <= 1
        all_singleton = all(
            (int(b[4]) if b[4] else 1) <= 1 for b in valid_buckets
        )

        if all_singleton:
            histogram["histogram-type"] = "singleton"
            for bucket_index, range_min, range_max, cardinality, unique_count in valid_buckets:
                cumulative_freq += (cardinality / total_freq)
                histogram["buckets"].append([
                    float(bucket_index + 1),
                    round(cumulative_freq, 5),
                ])
            return histogram

        # Equi-height path: check if ranges are numeric or binary-encoded
        try:
            float(valid_buckets[0][1])
            numeric_ranges = True
        except (ValueError, TypeError):
            numeric_ranges = False

        synthetic_start = 0
        for bucket_index, range_min, range_max, cardinality, unique_count in valid_buckets:
            cumulative_freq += (cardinality / total_freq)
            num_distinct = int(unique_count) if unique_count else 1

            if numeric_ranges:
                lo = float(range_min)
                hi = float(range_max)
            else:
                lo = float(synthetic_start)
                hi = float(synthetic_start + max(1, num_distinct) - 1)
                synthetic_start = int(hi) + 1

            histogram["buckets"].append([
                lo, hi,
                round(cumulative_freq, 5),
                num_distinct
            ])

        return histogram


class TiDBExtractor(SchemaExtractor):
    """Schema and statistics extractor for TiDB-compatible deployments."""

    DEFAULT_PORT = 4000
    TIDB_CLOUD_HOST_RE = re.compile(r"(^|\.)tidbcloud\.com$", re.IGNORECASE)

    @classmethod
    def is_cloud_host(cls, host):
        """Return True for TiDB Cloud gateway hostnames."""
        return bool(host and cls.TIDB_CLOUD_HOST_RE.search(host))

    @classmethod
    def build_connection_kwargs(cls, host, user, password, database, port=None, **overrides):
        kwargs = super().build_connection_kwargs(
            host,
            user,
            password,
            database,
            int(port or cls.DEFAULT_PORT),
            autocommit=True,
            allow_local_infile=True,
        )
        if cls.is_cloud_host(host):
            kwargs.update({
                "ssl_verify_cert": True,
                "ssl_verify_identity": True,
            })
            ssl_ca = os.environ.get("TIDB_SSL_CA")
            if ssl_ca:
                kwargs["ssl_ca"] = ssl_ca
            else:
                try:
                    import certifi
                    kwargs["ssl_ca"] = certifi.where()
                except ImportError:
                    pass
        kwargs.update(overrides)
        return kwargs

    def connect(self):
        """Establish a TiDB connection, enabling TLS verification for TiDB Cloud."""
        try:
            self.conn = mysql.connector.connect(**self.connection_kwargs())
            self.cursor = self.conn.cursor()
            print(f"Connected to TiDB database {self.database} at {self.host}")
            return True
        except Error as e:
            print(f"TiDB connection failed: {e}")
            return False

    def analyze_table(self, table):
        """Run TiDB ANALYZE TABLE to collect optimizer statistics."""
        self.cursor.execute(f"ANALYZE TABLE `{self.database}`.`{table}` ALL COLUMNS")
        self.cursor.fetchall()

    def get_column_histogram(self, table, column):
        """Return a TiDB column histogram in DataGenX internal format."""
        histogram_meta = self._stats_histogram_row(table, column, is_index=False)
        distinct_count = _safe_int(histogram_meta.get("distinct_count"), default=0)
        rows = self._show_stats_rows(
            "SHOW STATS_BUCKETS",
            table=table,
            column=column,
            is_index=False,
        )
        histogram = self._convert_tidb_buckets(rows, distinct_count)
        if histogram:
            return histogram
        return self._topn_to_singleton_histogram(table, column)

    def get_column_cardinalities(self, table):
        """Return TiDB column NDV estimates from SHOW STATS_HISTOGRAMS."""
        rows = self._show_stats_rows("SHOW STATS_HISTOGRAMS", table=table, is_index=False)
        cardinalities = {}
        for row in rows:
            column = row.get("column_name")
            distinct_count = row.get("distinct_count")
            if column and distinct_count is not None:
                cardinalities[column] = _safe_int(distinct_count, default=0)
        return cardinalities

    def get_column_topn(self, table, column):
        """Return TiDB TopN entries without exposing source literal values."""
        rows = self._show_stats_rows(
            "SHOW STATS_TOPN",
            table=table,
            column=column,
            is_index=False,
        )
        if not rows:
            return super().get_column_topn(table, column)

        counts = [
            _safe_int(row.get("count"), default=0)
            for row in rows
        ]
        total = sum(counts)
        if total <= 0:
            return []

        entries = []
        for ordinal, count in enumerate(counts, start=1):
            if count <= 0:
                continue
            entries.append(TopNEntry(
                ordinal=ordinal,
                frequency=count / total,
                count=count,
            ))
        return entries

    def get_index_cardinality(self, table):
        """Return TiDB index NDV estimates keyed by index name."""
        index_stats = {
            row.get("column_name"): row
            for row in self._show_stats_rows("SHOW STATS_HISTOGRAMS", table=table, is_index=True)
            if row.get("column_name")
        }
        try:
            self.cursor.execute("""
                SELECT INDEX_NAME, COLUMN_NAME, SEQ_IN_INDEX, CARDINALITY, NON_UNIQUE
                FROM INFORMATION_SCHEMA.STATISTICS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                ORDER BY INDEX_NAME, SEQ_IN_INDEX
            """, (self.database, table))
            base_indexes = _index_rows_to_cardinality(self.cursor.fetchall())
        except Exception:
            base_indexes = {}
        for index_name, entry in base_indexes.items():
            stats = index_stats.get(index_name)
            if stats:
                entry["cardinality"] = _safe_int(stats.get("distinct_count"), default=None)
                entry["null_count"] = _safe_int(stats.get("null_count"), default=None)
        return base_indexes

    def get_table_row_count(self, table):
        """Return TiDB statistics row count, falling back to exact count."""
        rows = self._show_stats_rows("SHOW STATS_META", table=table)
        if rows:
            non_partitioned = [
                row for row in rows
                if row.get("partition_name") in (None, "", "global")
            ]
            selected = non_partitioned or rows
            if len(selected) == 1:
                row_count = _safe_int(selected[0].get("row_count"), default=None)
            else:
                row_count = sum(
                    _safe_int(row.get("row_count"), default=0)
                    for row in selected
                    if row.get("partition_name") not in (None, "", "global")
                )
            if row_count:
                return row_count
        return super().get_table_row_count(table)

    def get_explain_plan(self, query, analyze=False):
        """Return TiDB EXPLAIN output.

        Non-analyze mode uses TiDB's structured JSON format. Analyze mode uses
        row output because EXPLAIN ANALYZE executes the query and returns
        runtime columns.
        """
        if analyze:
            return super().get_explain_plan(query, analyze=True)

        self.cursor.execute(f'EXPLAIN FORMAT = "tidb_json" {query}')
        rows = self.cursor.fetchall()
        raw = rows[0][0] if rows else "[]"
        try:
            plan = json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            plan = raw
        return {
            "format": "tidb_json",
            "analyze": False,
            "columns": list(self.cursor.column_names),
            "rows": rows,
            "plan": plan,
        }

    def _stats_histogram_row(self, table, column, is_index=False):
        rows = self._show_stats_rows(
            "SHOW STATS_HISTOGRAMS",
            table=table,
            column=column,
            is_index=is_index,
        )
        return rows[0] if rows else {}

    def _topn_to_singleton_histogram(self, table, column):
        rows = self._show_stats_rows(
            "SHOW STATS_TOPN",
            table=table,
            column=column,
            is_index=False,
        )
        if not rows:
            return None

        counts = [
            _safe_int(row.get("count"), default=0)
            for row in rows
        ]
        total = sum(counts)
        if total <= 0:
            return None

        cumulative = 0
        buckets = []
        for ordinal, count in enumerate(counts, start=1):
            if count <= 0:
                continue
            cumulative += count
            # Use synthetic ordinal values, never the real TopN values.
            buckets.append([ordinal, round(cumulative / total, 5)])

        if not buckets:
            return None
        return {
            "histogram-type": "singleton",
            "buckets": buckets,
        }

    def _show_stats_rows(self, statement, table=None, column=None, is_index=None):
        filters = [f"Db_name = {_sql_literal(self.database)}"]
        if table is not None:
            filters.append(f"Table_name = {_sql_literal(table)}")
        if column is not None:
            filters.append(f"Column_name = {_sql_literal(column)}")
        if is_index is not None:
            filters.append(f"Is_index = {1 if is_index else 0}")

        self.cursor.execute(f"{statement} WHERE {' AND '.join(filters)}")
        columns = [_normalize_column_name(col) for col in self.cursor.column_names]
        return [
            dict(zip(columns, row))
            for row in self.cursor.fetchall()
        ]

    @staticmethod
    def _convert_tidb_buckets(rows, distinct_count=0):
        """Convert TiDB SHOW STATS_BUCKETS rows to DataGenX histogram format."""
        if not rows:
            return None

        sorted_rows = sorted(rows, key=lambda row: _safe_int(row.get("bucket_id"), default=0))
        total_count = max(_safe_int(sorted_rows[-1].get("count"), default=0), 0)
        if total_count <= 0:
            return None

        histogram = {
            "histogram-type": "equi-height",
            "buckets": [],
        }

        previous_count = 0
        remaining_distinct = max(_safe_int(distinct_count, default=0), 0)
        remaining_rows = total_count

        for row in sorted_rows:
            cumulative_count = _safe_int(row.get("count"), default=previous_count)
            cumulative_count = max(cumulative_count, previous_count)
            bucket_rows = max(cumulative_count - previous_count, 0)
            if remaining_distinct > 0 and remaining_rows > 0:
                bucket_ndv = max(1, round(remaining_distinct * bucket_rows / remaining_rows))
                bucket_ndv = min(bucket_ndv, remaining_distinct)
                remaining_distinct -= bucket_ndv
                remaining_rows = max(remaining_rows - bucket_rows, 0)
            else:
                bucket_ndv = 1

            histogram["buckets"].append([
                _parse_tidb_bound(row.get("lower_bound")),
                _parse_tidb_bound(row.get("upper_bound")),
                round(cumulative_count / total_count, 5),
                bucket_ndv,
            ])
            previous_count = cumulative_count

        return histogram


def _index_rows_to_cardinality(rows):
    """Convert information_schema.statistics rows into a stable mapping."""
    indexes = {}
    for index_name, column_name, seq_in_index, cardinality, non_unique in rows:
        entry = indexes.setdefault(
            index_name,
            {
                "unique": not bool(non_unique),
                "columns": [],
            },
        )
        entry["columns"].append({
            "column": column_name,
            "seq_in_index": int(seq_in_index) if seq_in_index is not None else None,
            "cardinality": int(cardinality) if cardinality is not None else None,
        })
    return indexes


def _histogram_buckets_from_histogram(histogram):
    """Convert a native-ish histogram dict to neutral bucket objects."""
    if not histogram:
        return []

    hist_type = histogram.get("histogram-type")
    buckets = histogram.get("buckets") or []
    converted = []
    previous = 0.0

    for ordinal, bucket in enumerate(buckets, start=1):
        if hist_type == "singleton":
            if len(bucket) < 2:
                continue
            cumulative = _safe_float(bucket[1], default=previous)
            converted.append(HistogramBucket(
                ordinal=ordinal,
                lower_bound=bucket[0],
                upper_bound=bucket[0],
                frequency=max(0.0, cumulative - previous),
                cumulative_frequency=cumulative,
                num_distinct=1,
            ))
            previous = cumulative
            continue

        if hist_type == "equi-height":
            if len(bucket) < 4:
                continue
            cumulative = _safe_float(bucket[-2], default=previous)
            converted.append(HistogramBucket(
                ordinal=ordinal,
                lower_bound=bucket[0],
                upper_bound=bucket[1],
                frequency=max(0.0, cumulative - previous),
                cumulative_frequency=cumulative,
                num_distinct=_safe_int(bucket[-1], default=1),
            ))
            previous = cumulative

    return converted


def _topn_entries_from_histogram(histogram):
    """Expose singleton histograms as TopN-like entries without raw values."""
    buckets = _histogram_buckets_from_histogram(histogram)
    if not buckets or (histogram or {}).get("histogram-type") != "singleton":
        return []
    return [
        TopNEntry(
            ordinal=bucket.ordinal,
            frequency=bucket.frequency,
            count=None,
        )
        for bucket in buckets
    ]


def _normalize_column_name(name):
    return str(name).strip().lower()


def _safe_float(value, default=0.0):
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value, default=0):
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return default


def _parse_tidb_bound(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return value
    text = str(value).strip()
    if not text:
        return text
    try:
        as_float = float(text)
    except ValueError:
        return text
    if as_float.is_integer() and "." not in text and "e" not in text.lower():
        return int(as_float)
    return as_float


def _sql_literal(value):
    escaped = str(value).replace("\\", "\\\\").replace("'", "''")
    return f"'{escaped}'"


EXTRACTOR_TYPES = {
    "mysql": MySQLExtractor,
    "singlestore": SingleStoreExtractor,
    "tidb": TiDBExtractor,
}


def available_extractor_types():
    """Return supported extractor type names."""
    return tuple(sorted(EXTRACTOR_TYPES))


def create_schema_extractor(db_type, host, user, password, database, port=None):
    """Create a schema/statistics extractor for a supported database type."""
    try:
        extractor_cls = EXTRACTOR_TYPES[db_type]
    except KeyError as exc:
        supported = ", ".join(available_extractor_types())
        raise ValueError(f"Unsupported database type {db_type!r}; expected one of: {supported}") from exc
    return extractor_cls(host, user, password, database, port)


def connection_kwargs_for(db_type, host, user, password, database, port=None, **overrides):
    """Build mysql.connector connection keyword arguments for a supported type."""
    try:
        extractor_cls = EXTRACTOR_TYPES[db_type]
    except KeyError as exc:
        supported = ", ".join(available_extractor_types())
        raise ValueError(f"Unsupported database type {db_type!r}; expected one of: {supported}") from exc
    return extractor_cls.build_connection_kwargs(
        host,
        user,
        password,
        database,
        port,
        **overrides,
    )

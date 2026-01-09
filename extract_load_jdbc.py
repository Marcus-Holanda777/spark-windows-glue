from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
import pyspark.sql.types as tp
import pyspark.sql.functions as fn
from datetime import datetime, timedelta
from dataclasses import dataclass
import uuid
from prefect import task, flow
import re
from athena_mvsh import Athena, CursorParquetDuckdb
import shutil

local_files = "M:/lake_house"


@dataclass
class SparkTable:
    server: str
    database: str
    schema_table: str
    table_name: str
    primary_key: str
    conds: str = None

    @property
    def full_name(self) -> str:
        return f"{self.database}.{self.schema_table}.{self.table_name}"

    @property
    def full_name_athena(self) -> str:
        return f"{self.database.lower()}_{self.schema_table.lower()}_{self.table_name.lower()}"


@dataclass
class URLMssql:
    table: SparkTable
    driver: str = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    isolation_level: str = "READ_UNCOMMITTED"
    prefer_timestamp_ntz: bool = True

    @property
    def full_url(self) -> str:
        return (
            f"jdbc:sqlserver://{self.table.server}:1433;"
            f"databaseName={self.table.database};"
            "encrypt=true;integratedSecurity=true;trustServerCertificate=true;"
        )

    @property
    def options(self) -> dict[str, str]:
        return {
            "url": self.full_url,
            "driver": self.driver,
            "isolationLevel": self.isolation_level,
            "preferTimestampNTZ": self.prefer_timestamp_ntz,
        }


def parse_date_expressions(query_string: str | None) -> str | None:
    if not query_string:
        return query_string

    hoje = datetime.now()
    bases = {
        "hoje": hoje,
        "ontem": hoje - timedelta(days=1),
        "inicio_mes": hoje.replace(day=1),
        "inicio_ano": hoje.replace(month=1, day=1),
    }

    def replace_match(match: re.Match):
        base_name = match.group(1)
        operator = match.group(2)
        amount = match.group(3)

        target_date = bases.get(base_name)
        if not target_date:
            return match.group(0)

        if not operator:
            return target_date.strftime("%Y-%m-%d")

        days = int(amount)
        if operator == "-":
            new_date = target_date - timedelta(days=days)
        elif operator == "+":
            new_date = target_date + timedelta(days=days)

        return new_date.strftime("%Y-%m-%d")

    pattern = r"\{(hoje|ontem|inicio_mes|inicio_ano)(?:([\+\-])(\d+)d)?\}"
    return re.sub(pattern, replace_match, query_string)


@task(log_prints=True)
def lower_upper_bound(spark: SparkSession, table: SparkTable) -> tuple:
    url = URLMssql(table)

    stmt = f"""
    select 
        min({table.primary_key}) as min_lower, 
        max({table.primary_key}) as max_upper
    from {table.full_name} with(nolock)
    {f"where {table.conds}" if table.conds else ""}
    """

    options = url.options
    options |= {"query": stmt}

    start, end = spark.read.format("jdbc").options(**options).load().collect()[0]

    return start, end


@task(log_prints=True)
def write_parquet(spark: SparkSession, table: SparkTable, cores: int = 8) -> str:
    start, end = lower_upper_bound(spark, table)
    url = URLMssql(table)

    if isinstance(start, datetime):
        start, end = f"{start:%Y-%m-%d %H:%M:%S}", f"{end:%Y-%m-%d %H:%M:%S}"

    options = url.options

    options |= {
        "dbtable": f"(select * from {table.full_name} with(nolock) {f'where {table.conds}' if table.conds else ''}) as tabela",
        "partitionColumn": table.primary_key,
        "numPartitions": cores,
        "fetchsize": 100_000,
        "lowerBound": f"{start}",
        "upperBound": f"{end}",
    }

    df = spark.read.format("jdbc").options(**options).load()
    rename_cols = {c: c.lower() for c in df.columns}

    df.withColumnsRenamed(rename_cols).write.parquet(
        f"{local_files}/{table.full_name_athena}/{table.full_name_athena}_{uuid.uuid4()}",
        compression="zstd",
        mode="overwrite",
    )

    return table


@task(log_prints=True)
def overwrite_table_iceberg(
    spark: SparkSession, table: SparkTable, schema: str = "spark_load"
) -> SparkTable:
    df = spark.read.parquet(
        f"{local_files}/{table.full_name_athena}/{table.full_name_athena}_*"
    )

    (
        df.coalesce(24)
        .writeTo(f"{schema}.{table.full_name_athena}")
        .tableProperty("write.format.default", "parquet")
        .tableProperty("write.parquet.compression-codec", "zstd")
        .tableProperty("write.parquet.compression-level", "3")
        .tableProperty("write.distribution-mode", "none")
        .tableProperty("write.object-storage.enabled", "true")
        .tableProperty("write.target-file-size-bytes", "134217728")  # 128MB
        .tableProperty("write.delete.mode", "merge-on-read")
        .tableProperty("write.update.mode", "merge-on-read")
        .tableProperty("write.merge.mode", "merge-on-read")
        .createOrReplace()
    )

    return table


@task(log_prints=True)
def optimize_table(
    spark: SparkSession, table: SparkTable, schema: str = "spark_load"
) -> SparkTable:
    print(" => rewrite_data_files")
    table_name = f"{schema}.{table.full_name_athena}"
    spark.sql(f"""
        CALL system.rewrite_data_files(
            table => '{table_name}',
            options => map(
                'partial-progress.enabled', 'true',
                'rewrite-all', 'true'
            )
        )
    """)

    print(" => expire_snapshots")
    dt_expire = datetime.now() - timedelta(minutes=1)
    spark.sql(f"""
        CALL system.expire_snapshots(
            table => '{table_name}',
            older_than => TIMESTAMP '{dt_expire:%Y-%m-%d %H:%M:%S}',
            retain_last => 1
        )
    """)

    print(" => remove_orphan_files")
    spark.sql(f"""
        CALL system.remove_orphan_files(
            table => '{table_name}',
            dry_run => false
        )
    """)

    return table


@task(log_prints=True)
def iter_lower_upper_bound(
    spark: SparkSession, table: SparkTable, chunk: int = 100
) -> list[tuple]:
    url = URLMssql(table)

    stmt = f"""
    select 
        distinct {table.primary_key} as chave
    from {table.full_name} with(nolock)
    {f"where {table.conds}" if table.conds else ""}
    """

    options = url.options
    options |= {"query": stmt}

    rows = spark.read.format("jdbc").options(**options).load().collect()
    rows_sorted = sorted([row.chave for row in rows])

    lista_batch = []

    for idx in range(0, len(rows_sorted), chunk):
        lotes = rows_sorted[idx : idx + chunk]
        lista_batch.append((lotes[0], lotes[-1]))

    return lista_batch


@task(log_prints=True)
def merge_table(
    spark: SparkSession,
    tbl: SparkTable,
    predicate: str,
    schema_target: str = "prevencao-perdas",
    schema_source: str = "spark_load",
    alias: tuple = ("t", "s"),
    drop_table: bool = False,
) -> None:
    table = spark.table(f"{schema_source}.{tbl.full_name_athena}")
    columns = table.columns

    cols = list(map(lambda col: f'"{col}"', columns))
    target, source = alias
    update_cols = ", ".join(f"{col} = {source}.{col}" for col in cols)
    insert_cols = ", ".join(cols)
    values_cols = ", ".join(f"{source}.{col}" for col in cols)

    stmt = f"""
        MERGE INTO "{schema_target}"."{tbl.full_name_athena}" AS {target}
        USING "{schema_source}"."{tbl.full_name_athena}" AS {source}
        ON ({predicate})
        WHEN MATCHED
            THEN UPDATE SET {update_cols}
        WHEN NOT MATCHED
            THEN INSERT ({insert_cols}) VALUES ({values_cols})
    """

    with Athena(
        cursor=CursorParquetDuckdb(
            s3_staging_dir="s3://out-of-lake-pmenos-query-results/"
        )
    ) as client:
        client.execute(stmt)
        if drop_table:
            client.execute(
                f"DROP TABLE IF EXISTS {schema_source}.{tbl.full_name_athena}"
            )
        client.execute(
            f"""OPTIMIZE {schema_target}.{tbl.full_name_athena} REWRITE DATA USING BIN_PACK"""
        )
        client.execute(f"""VACUUM {schema_target}.{tbl.full_name_athena}""")


@task(log_prints=True)
def create_table_as_full(
    tbl: SparkTable,
    schema_target: str = "prevencao-perdas",
    schema_source: str = "spark_load",
    drop_table: bool = True,
) -> None:
    drop_table_as = "drop table if exists `{schema}`.{table}"

    stmt_create_as = f"""
    CREATE TABLE "{schema_target}".{tbl.full_name_athena}
    WITH (
        table_type = 'ICEBERG',
        is_external = FALSE,
        format = 'PARQUET',
        write_compression = 'ZSTD',
        location = 's3://out-of-lake-prevencao-perdas/tables/{tbl.full_name_athena}/{uuid.uuid4()}/'
    ) AS
    SELECT * FROM {schema_source}.{tbl.full_name_athena}
    """

    with Athena(
        cursor=CursorParquetDuckdb(
            s3_staging_dir="s3://out-of-lake-pmenos-query-results/"
        )
    ) as client:
        client.execute(
            drop_table_as.format(schema=schema_target, table=tbl.full_name_athena)
        )
        client.execute(stmt_create_as)
        if drop_table:
            client.execute(
                drop_table_as.format(schema=schema_source, table=tbl.full_name_athena)
            )


@flow(log_prints=True)
def main_spark_jdbc(
    server: str,
    database: str,
    schema_table: str,
    table_name: str,
    primary_key: str,
    conds: str | None = None,
    chunk: int = 100,
    cores: int = 8,
    optimize: bool = False,
    iter_lower: bool = True,
    create_table: bool = True,
    schema_target: str = "prevencao-perdas",
    schema_source: str = "spark_load",
    predicate_merge: str | None = None,
    create_table_as: bool = False,
    drop_table: bool = False,
) -> None:
    """executar no worker PDP0457"""

    try:
        spark: SparkSession = SparkSession.builder.appName("job_jdbc").getOrCreate()
        conds = parse_date_expressions(conds)

        tbl = SparkTable(
            server=server,
            database=database,
            schema_table=schema_table,
            table_name=table_name,
            primary_key=primary_key,
            conds=conds,
        )

        if iter_lower:
            rows = iter_lower_upper_bound(spark, tbl, chunk=chunk)
            where = f"and ({conds})" if conds else ""
            for start, end in rows:
                tbl.conds = f"{tbl.primary_key} between {start} and {end} {where}"
                print(tbl.conds)
                print("1 - WRITE PARQUET")
                write_parquet(spark, tbl, cores)
        else:
            write_parquet(spark, tbl, cores)

        if create_table:
            print("2 - OVERWRITE TABLE ICEBERG")
            overwrite_table_iceberg(spark, tbl)

        if create_table_as:
            print("3 - CREATE TABLE AS")
            create_table_as_full(tbl, schema_target, schema_source, drop_table)

        if predicate_merge and not create_table_as:
            print(f"4 - MERGE TABLE ICEBERG -> athena, DROP: {drop_table}")
            merge_table(
                spark,
                tbl,
                predicate=predicate_merge,
                schema_source=schema_source,
                drop_table=drop_table,
            )

        if optimize and not drop_table:
            print("5 - OPTIMIZE TABLE")
            optimize_table(spark, tbl)
    except Exception as e:
        print(e)
        raise
    finally:
        shutil.rmtree(f"{local_files}/{tbl.full_name_athena}")
        spark.stop()

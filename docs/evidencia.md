# Evidencia de la verificación (2026-11-03, us-east-2)

Despliegue con `name_prefix = lh-hgabaldo` en la cuenta 659500704179, región **us-east-2**.
Se enviaron 100 eventos con `scripts/put_events.sh lh-hgabaldo-eventos 100` (20 por región)
y se verificó la tabla con Athena (workgroup `lh-hgabaldo`) y la AWS CLI.

## Glue Data Catalog

```text
$ aws glue get-database --name lakehouse_db --region us-east-2
{
    "Name": "lakehouse_db",
    "CatalogId": "659500704179",
    "CreateTime": "2026-11-03T22:34:29-03:00"
}

$ aws glue get-table --database-name lakehouse_db --name eventos --region us-east-2
{
    "Name": "eventos",
    "TableType": "EXTERNAL_TABLE",
    "Location": "s3://lh-hgabaldo-659500704179/warehouse/lakehouse_db.db/eventos",
    "Columns": [
        { "Name": "event_id",        "Type": "string" },
        { "Name": "event_timestamp", "Type": "timestamp" },
        { "Name": "region",          "Type": "string" }
    ],
    "table_type": "ICEBERG",
    "metadata_location": "s3://lh-hgabaldo-659500704179/warehouse/lakehouse_db.db/eventos/metadata/00004-7a166c11-9860-47cf-9aa1-808eb08a1aa6.metadata.json"
}
```

`table_type = ICEBERG` y `metadata_location` los registra `GlueCatalog` en cada commit; ningún crawler interviene.

## Bucket con versionado

```text
$ aws s3api get-bucket-versioning --bucket lh-hgabaldo-659500704179
{
    "Status": "Enabled"
}
```

## Athena

El conteo coincide con los eventos enviados (100):

```sql
SELECT region, count(*) AS eventos FROM lakehouse_db.eventos GROUP BY region ORDER BY region;
```

| region   | eventos |
|----------|--------:|
| af-south | 20 |
| ap-south | 20 |
| eu-west  | 20 |
| sa-east  | 20 |
| us-east  | 20 |

Un snapshot `append` por cada checkpoint de Flink (60 s) que trajo datos:

```sql
SELECT snapshot_id, committed_at, operation FROM "lakehouse_db"."eventos$snapshots" ORDER BY committed_at;
```

| snapshot_id         | committed_at                | operation |
|---------------------|-----------------------------|-----------|
| 1528536136177882963 | 2026-11-04 02:32:20.092 UTC | append |
| 614310893381697353  | 2026-11-04 02:33:18.343 UTC | append |
| 5261234275917034510 | 2026-11-04 02:35:18.411 UTC | append |
| 5683417075778186554 | 2026-11-04 02:35:18.922 UTC | append |

Todas las filas caen en una sola partición diaria (hidden partitioning con `days(event_timestamp)`):

```sql
SELECT partition, record_count, file_count FROM "lakehouse_db"."eventos$partitions";
```

| partition                          | record_count | file_count |
|------------------------------------|-------------:|-----------:|
| {event_timestamp_day=2026-11-04}   | 100 | 4 |

## Archivos de la tabla en S3

`metadata/` contiene un `.metadata.json` por commit (más el inicial de la creación), las manifest
lists (`snap-*.avro`) y los manifest files (`*-m0.avro`); `data/` contiene los Parquet bajo la
partición del día.

```text
$ aws s3 ls s3://lh-hgabaldo-659500704179/warehouse/lakehouse_db.db/eventos/ --recursive
2026-11-03 23:32:19 1288 data/event_timestamp_day=2026-11-04/00000-0-acb61428-...-00001.parquet
2026-11-03 23:33:18 1405 data/event_timestamp_day=2026-11-04/00000-0-acb61428-...-00002.parquet
2026-11-03 23:34:18 1405 data/event_timestamp_day=2026-11-04/00000-0-acb61428-...-00003.parquet
2026-11-03 23:35:18 1309 data/event_timestamp_day=2026-11-04/00000-0-acb61428-...-00004.parquet
2026-11-03 23:11:37 1269 metadata/00000-2a78c897-da93-498e-8b71-1ba58687cb2a.metadata.json
2026-11-03 23:32:21 2686 metadata/00001-ac89c099-f836-4460-9484-03be5e6d221d.metadata.json
2026-11-03 23:33:19 4042 metadata/00002-0216bbf7-afae-4e03-8de8-62aff06cb62f.metadata.json
2026-11-03 23:35:19 5404 metadata/00003-3e1b6302-0d79-488c-8bb2-a8f14c5050c3.metadata.json
2026-11-03 23:35:19 6766 metadata/00004-7a166c11-9860-47cf-9aa1-808eb08a1aa6.metadata.json
2026-11-03 23:32:21 4481 metadata/snap-1528536136177882963-1-84aa4b45-....avro
2026-11-03 23:33:19 4558 metadata/snap-614310893381697353-1-f5fbc7e6-....avro
2026-11-03 23:35:19 4604 metadata/snap-5261234275917034510-1-3107e664-....avro
2026-11-03 23:35:19 4653 metadata/snap-5683417075778186554-1-3116101d-....avro
2026-11-03 23:32:20 7117 metadata/84aa4b45-7e29-488b-9ca4-852b20c53fcc-m0.avro
2026-11-03 23:33:19 7115 metadata/f5fbc7e6-b51c-46c2-839c-336f353eaa94-m0.avro
2026-11-03 23:35:19 7117 metadata/3107e664-d701-49f0-8810-00fd51376a9a-m0.avro
2026-11-03 23:35:19 7116 metadata/3116101d-4e47-4f16-95d5-fd1b5dacaf6d-m0.avro
```

Los cuatro Parquet de pocos KB (uno por commit) son el small files problem en miniatura: en
producción se compactarían con `OPTIMIZE lakehouse_db.eventos REWRITE DATA USING BIN_PACK`
sobre particiones cerradas.

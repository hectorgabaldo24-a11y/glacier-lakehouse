# Lakehouse: Kinesis → Flink → Iceberg (Glue) → Athena

Pre-entrega 5. Flink (Amazon Managed Service for Apache Flink) lee eventos JSON de Kinesis y los escribe en la
tabla Iceberg `lakehouse_db.eventos`, registrada en el Glue Data Catalog y consultable desde Athena.

```
infra/    Terraform: bucket (versionado), Glue DB, Kinesis, IAM, app de Flink, workgroup de Athena
app/      Flink job (Java 11, Maven): GlueCatalog + S3FileIO + IcebergSink
scripts/  put_events.sh: productor de prueba
```

## Cómo correrlo

```bash
# 1. Jar de Flink (Terraform lo sube a S3, así que va primero)
mvn -f app package                      # genera app/target/lakehouse-flink-1.0.jar

# 2. Infraestructura
cd infra
terraform init
terraform apply -var account_id=<TU_ACCOUNT_ID> -var name_prefix=<prefijo-unico>

# 3. Eventos
cd ..
scripts/put_events.sh <stream-name> 100  # stream-name: output kinesis_stream
```

Esperá al próximo checkpoint (hasta 1 min) y consultá en Athena (workgroup = output `athena_workgroup`):

```sql
SELECT region, count(*) AS eventos FROM lakehouse_db.eventos GROUP BY region;
SELECT snapshot_id, committed_at FROM "lakehouse_db"."eventos$snapshots";
```

Verificaciones:

```bash
aws glue get-database --name lakehouse_db
aws s3api get-bucket-versioning --bucket <lakehouse_bucket>
aws glue get-table --database-name lakehouse_db --name eventos   # Parameters.table_type = ICEBERG
aws s3 ls s3://<lakehouse_bucket>/warehouse/lakehouse_db.db/eventos/ --recursive
```

Limpieza: `terraform destroy` (los buckets tienen `force_destroy`, solo para el demo).

## Particionado

`days(event_timestamp)`: genera un valor por día (cardinalidad baja y pareja), es la columna que filtran las
consultas, y el hidden partitioning permite filtrar por `event_timestamp` sin columnas derivadas. Evita el
sobre-particionamiento de claves como `event_id` y el data skew de `region`.

## Evidencia

_Agregar captura de `lakehouse_db` en la consola de Glue y del resultado de Athena._

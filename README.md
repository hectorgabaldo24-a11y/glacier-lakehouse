# Lakehouse: Kinesis → Flink → Iceberg (Glue) → Athena

Pre-entrega 5. Flink (Amazon Managed Service for Apache Flink) lee eventos JSON de Kinesis y los escribe en la
tabla Iceberg `lakehouse_db.eventos`, registrada en el Glue Data Catalog y consultable desde Athena.

```
infra/    Terraform: bucket (versionado), Glue DB, Kinesis, IAM, app de Flink, workgroup de Athena
app/      Flink job (Java 11, Maven): GlueCatalog + S3FileIO + IcebergSink
scripts/  put_events.sh: productor de prueba
docs/     evidencia de la verificación (capturas y salidas de Athena/Glue/S3)
```

## Cómo correrlo

```bash
# 1. Jar de Flink (Terraform lo sube a S3, así que va primero)
mvn -f app package                      # genera app/target/lakehouse-flink-1.0.jar

# 2. Infraestructura
#    Con credenciales de `aws login`, el provider de Terraform no las lee directamente:
#    exportarlas antes (duran ~15 min).
eval "$(aws configure export-credentials --format env)"
cd infra
terraform init
terraform apply -var account_id=<TU_ACCOUNT_ID> -var name_prefix=<prefijo-unico> -var region=us-east-2

# 3. Eventos (después de que la app de Flink esté RUNNING: la fuente lee desde LATEST)
cd ..
AWS_REGION=us-east-2 scripts/put_events.sh <stream-name> 100  # stream-name: output kinesis_stream
```

> La región por defecto es `us-east-1`. El despliegue de la entrega se hizo en `us-east-2` porque en
> `us-east-1` la cuenta ya tiene otra `lakehouse_db` de otro proyecto: el catálogo de Glue es regional,
> así que en `us-east-2` el nombre queda libre sin tocar nada ajeno.

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

Limpieza: `terraform destroy` con las mismas variables (los buckets tienen `force_destroy`, solo para el demo).

## Particionado

`days(event_timestamp)`: genera un valor por día (cardinalidad baja y pareja), es la columna que filtran las
consultas, y el hidden partitioning permite filtrar por `event_timestamp` sin columnas derivadas. Evita el
sobre-particionamiento de claves como `event_id` y el data skew de `region`.

## Evidencia

Verificación del 2026-11-03 en `us-east-2`: 100 eventos enviados, 100 filas en Athena (20 por región),
4 snapshots `append` (uno por checkpoint con datos) y Parquet bajo `event_timestamp_day=2026-11-04`.
Salidas completas de Glue, Athena y S3 en [docs/evidencia.md](docs/evidencia.md).

![lakehouse_db en la consola de Glue](docs/glue-lakehouse_db.png)

![Tabla eventos registrada como Iceberg](docs/glue-eventos.png)

![Conteo por región en Athena](docs/athena-conteo.png)

![Snapshots de la tabla en Athena](docs/athena-snapshots.png)

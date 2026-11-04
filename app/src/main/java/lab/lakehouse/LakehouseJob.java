package lab.lakehouse;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kinesis.source.KinesisStreamsSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.IcebergSink;
import org.apache.iceberg.types.Types;

public class LakehouseJob {

    public static void main(String[] args) throws Exception {
        Properties p = KinesisAnalyticsRuntime.getApplicationProperties().get("LakehouseConfig");
        String db = p.getProperty("glue.db");
        String table = p.getProperty("glue.table");

        // Paso 3: GlueCatalog + S3FileIO. Operators get the loaders, never the Catalog (not serializable).
        Map<String, String> props = new HashMap<>();
        props.put("warehouse", p.getProperty("warehouse"));
        props.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        CatalogLoader catalogLoader = CatalogLoader.custom(
                "glue", props, new org.apache.hadoop.conf.Configuration(),
                "org.apache.iceberg.aws.glue.GlueCatalog");

        Catalog catalog = catalogLoader.loadCatalog();
        System.out.println("Tables in " + db + ": " + catalog.listTables(Namespace.of(db)));

        // Paso 4: table partitioned by day on the event time. GlueCatalog also sets table_type=ICEBERG in Glue.
        TableIdentifier id = TableIdentifier.of(db, table);
        if (!catalog.tableExists(id)) {
            Schema schema = new Schema(
                    Types.NestedField.required(1, "event_id", Types.StringType.get()),
                    Types.NestedField.required(2, "event_timestamp", Types.TimestampType.withZone()),
                    Types.NestedField.optional(3, "region", Types.StringType.get()));
            PartitionSpec spec = PartitionSpec.builderFor(schema).day("event_timestamp").build();
            catalog.createTable(id, schema, spec);
        }
        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, id);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // One checkpoint per minute = one Iceberg commit and one snapshot per minute.
        env.enableCheckpointing(60_000);

        Configuration sourceConfig = new Configuration();
        sourceConfig.setString("aws.region", p.getProperty("aws.region"));
        KinesisStreamsSource<String> source = KinesisStreamsSource.<String>builder()
                .setStreamArn(p.getProperty("stream.arn"))
                .setSourceConfig(sourceConfig)
                .setDeserializationSchema(new SimpleStringSchema())
                .build();

        DataStream<RowData> rows = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "kinesis-eventos")
                .flatMap(new EventParser())
                .returns(TypeInformation.of(RowData.class));

        IcebergSink.forRowData(rows)
                .tableLoader(tableLoader)
                .append();

        env.execute("lakehouse-eventos");
    }
}

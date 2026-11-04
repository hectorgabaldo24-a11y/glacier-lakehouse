resource "aws_cloudwatch_log_group" "flink" {
  name              = "/aws/kinesis-analytics/${var.name_prefix}"
  retention_in_days = 7
}

resource "aws_cloudwatch_log_stream" "flink" {
  name           = "app"
  log_group_name = aws_cloudwatch_log_group.flink.name
}

resource "aws_s3_object" "app_jar" {
  bucket = aws_s3_bucket.artifacts.id
  key    = "app/lakehouse-flink.jar"
  source = var.app_jar_path
  etag   = filemd5(var.app_jar_path)
}

resource "aws_kinesisanalyticsv2_application" "lakehouse" {
  name                   = "${var.name_prefix}-flink"
  runtime_environment    = "FLINK-1_19"
  service_execution_role = aws_iam_role.flink.arn
  start_application      = true

  application_configuration {
    application_code_configuration {
      code_content_type = "ZIPFILE"
      code_content {
        s3_content_location {
          bucket_arn     = aws_s3_bucket.artifacts.arn
          file_key       = aws_s3_object.app_jar.key
          object_version = aws_s3_object.app_jar.version_id
        }
      }
    }

    environment_properties {
      property_group {
        property_group_id = "LakehouseConfig"
        property_map = {
          "stream.arn" = aws_kinesis_stream.eventos.arn
          "aws.region" = var.region
          "warehouse"  = "s3://${aws_s3_bucket.lakehouse.id}/warehouse"
          "glue.db"    = aws_glue_catalog_database.lakehouse.name
          "glue.table" = "eventos"
        }
      }
    }

    flink_application_configuration {
      # CUSTOM, otherwise Managed Flink ignores env.enableCheckpointing() in the code.
      checkpoint_configuration {
        configuration_type    = "CUSTOM"
        checkpointing_enabled = true
        checkpoint_interval   = 60000
      }

      monitoring_configuration {
        configuration_type = "CUSTOM"
        log_level          = "INFO"
        metrics_level      = "APPLICATION"
      }

      parallelism_configuration {
        configuration_type   = "CUSTOM"
        auto_scaling_enabled = false
        parallelism          = 1
        parallelism_per_kpu  = 1
      }
    }
  }

  cloudwatch_logging_options {
    log_stream_arn = aws_cloudwatch_log_stream.flink.arn
  }

  depends_on = [
    aws_iam_role_policy_attachment.flink_lakehouse,
    aws_iam_role_policy_attachment.flink_runtime,
  ]
}

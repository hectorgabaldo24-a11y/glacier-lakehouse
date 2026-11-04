output "lakehouse_bucket" {
  value = aws_s3_bucket.lakehouse.id
}

output "kinesis_stream" {
  value = aws_kinesis_stream.eventos.name
}

output "athena_workgroup" {
  value = aws_athena_workgroup.lakehouse.name
}

output "flink_app" {
  value = aws_kinesisanalyticsv2_application.lakehouse.name
}

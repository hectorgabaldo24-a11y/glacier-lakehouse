resource "aws_athena_workgroup" "lakehouse" {
  name          = var.name_prefix
  force_destroy = true

  configuration {
    enforce_workgroup_configuration = true

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }

    result_configuration {
      output_location = "s3://${aws_s3_bucket.artifacts.id}/athena-results/"
    }
  }
}

resource "aws_s3_bucket" "lakehouse" {
  bucket        = "${var.name_prefix}-${var.account_id}"
  force_destroy = true # demo only: lets `terraform destroy` empty a versioned bucket
}

resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Flink application jar and Athena query results live in a separate bucket
# so the lakehouse bucket only holds table data and metadata.
resource "aws_s3_bucket" "artifacts" {
  bucket        = "${var.name_prefix}-artifacts-${var.account_id}"
  force_destroy = true
}

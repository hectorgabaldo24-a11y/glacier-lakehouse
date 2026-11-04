data "aws_iam_policy_document" "flink_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["kinesisanalytics.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "flink" {
  name               = "${var.name_prefix}-flink"
  assume_role_policy = data.aws_iam_policy_document.flink_assume.json
}

# Paso 2: minimum permissions to create/read/update Iceberg tables in Glue
# and to read/write objects in the lakehouse bucket.
data "aws_iam_policy_document" "flink_lakehouse" {
  statement {
    # GetTables: GlueCatalog.listTables() (startup check in LakehouseJob) calls glue:GetTables.
    actions = ["glue:GetDatabase", "glue:GetTable", "glue:GetTables", "glue:CreateTable", "glue:UpdateTable"]
    resources = [
      "arn:aws:glue:${var.region}:${var.account_id}:catalog",
      aws_glue_catalog_database.lakehouse.arn,
      "arn:aws:glue:${var.region}:${var.account_id}:table/lakehouse_db/*",
    ]
  }

  statement {
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.lakehouse.arn, "${aws_s3_bucket.lakehouse.arn}/*"]
  }
}

resource "aws_iam_policy" "flink_lakehouse" {
  name   = "${var.name_prefix}-flink-lakehouse"
  policy = data.aws_iam_policy_document.flink_lakehouse.json
}

resource "aws_iam_role_policy_attachment" "flink_lakehouse" {
  role       = aws_iam_role.flink.name
  policy_arn = aws_iam_policy.flink_lakehouse.arn
}

# Module 4 permissions: read the stream, fetch the jar, write logs.
data "aws_iam_policy_document" "flink_runtime" {
  statement {
    actions = [
      "kinesis:DescribeStream",
      "kinesis:DescribeStreamSummary",
      "kinesis:GetRecords",
      "kinesis:GetShardIterator",
      "kinesis:ListShards",
      "kinesis:SubscribeToShard",
      "kinesis:DescribeStreamConsumer",
      "kinesis:RegisterStreamConsumer",
      "kinesis:ListStreamConsumers",
    ]
    resources = [aws_kinesis_stream.eventos.arn, "${aws_kinesis_stream.eventos.arn}/*"]
  }

  statement {
    actions   = ["s3:GetObject", "s3:GetObjectVersion"]
    resources = ["${aws_s3_bucket.artifacts.arn}/app/*"]
  }

  statement {
    actions   = ["logs:DescribeLogGroups"]
    resources = ["arn:aws:logs:${var.region}:${var.account_id}:log-group:*"]
  }

  statement {
    actions   = ["logs:DescribeLogStreams", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.flink.arn}:*"]
  }
}

resource "aws_iam_policy" "flink_runtime" {
  name   = "${var.name_prefix}-flink-runtime"
  policy = data.aws_iam_policy_document.flink_runtime.json
}

resource "aws_iam_role_policy_attachment" "flink_runtime" {
  role       = aws_iam_role.flink.name
  policy_arn = aws_iam_policy.flink_runtime.arn
}

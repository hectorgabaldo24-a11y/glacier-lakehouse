resource "aws_kinesis_stream" "eventos" {
  name             = "${var.name_prefix}-eventos"
  retention_period = 24

  stream_mode_details {
    stream_mode = "ON_DEMAND"
  }
}

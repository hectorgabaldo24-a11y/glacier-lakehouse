variable "region" {
  type    = string
  default = "us-east-1"
}

variable "account_id" {
  description = "AWS account ID, used to build the Glue ARNs."
  type        = string
}

variable "name_prefix" {
  description = "Prefix for bucket and resource names. Bucket names are global: make it unique."
  type        = string
  default     = "lakehouse-demo"
}

variable "app_jar_path" {
  description = "Path to the shaded Flink jar built with `mvn -f app package`."
  type        = string
  default     = "../app/target/lakehouse-flink-1.0.jar"
}

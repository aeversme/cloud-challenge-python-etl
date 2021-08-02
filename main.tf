terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 3.27"
    }
    random = {
      source  = "hashicorp/random"
      version = "2.3.0"
    }
  }

  required_version = ">= 0.14.9"

  backend "remote" {
    organization = "ae-cloud-dev"

    workspaces {
      name = "python-etl-project"
    }
  }
}

provider "aws" {
  profile = "iamadmin-general"
  region  = "us-east-1"
}

locals {
  lambda_layer_source = "Klayers-python38-pandas.zip"
}

resource "random_id" "id" {
  byte_length = 8
}

resource "aws_s3_bucket" "bucket" {
  bucket = "python-etl-lambda-deployment-bucket-${random_id.id.hex}"
  acl    = "private"
}

resource "aws_s3_bucket_object" "file_upload" {
  bucket = aws_s3_bucket.bucket.id
  key    = "Klayers-python38-pandas.zip"
  source = local.lambda_layer_source
}

module "lambda_function" {
  source = "terraform-aws-modules/lambda/aws"

  function_name = "python-covid-etl"
  description   = "Lambda function for Python ETL job with Pandas layer, deployed with Terraform"
  handler       = "etl.lambda_handler"
  runtime       = "python3.8"
  timeout       = 120
  publish       = true

  source_path = "./etl.py"

  store_on_s3             = true
  s3_bucket               = aws_s3_bucket.bucket.id
  s3_object_storage_class = "STANDARD"

  layers = [
    module.lambda_layer_s3.lambda_layer_arn
  ]

  tags = {
    Module = "lambda-with-layer"
  }
}

module "lambda_layer_s3" {
  source = "terraform-aws-modules/lambda/aws"

  create_layer = true

  layer_name          = "pandas_lambda_layer"
  description         = "Pandas layer for Lambda, created using Terraform"
  compatible_runtimes = ["python3.8"]

  create_package = false
  s3_existing_package = {
    bucket = aws_s3_bucket.bucket.id
    key    = aws_s3_bucket_object.file_upload.key
  }
}

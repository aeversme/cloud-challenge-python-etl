terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 3.27"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.1.0"
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

  source_path = [
    "./etl.py",
    "./data_handler.py",
    "./data_transformer.py",
    "./message_handler.py"
  ]

  store_on_s3             = true
  s3_bucket               = aws_s3_bucket.bucket.id
  s3_object_storage_class = "STANDARD"

  layers = [
    module.lambda_layer_s3.lambda_layer_arn
  ]

  attach_policy_statements = true
  policy_statements = {
    sns = {
      effect = "Allow",
      actions = [
        "sns:ListTopics"
      ],
      resources = ["arn:aws:sns:us-east-1:033979438744:*"]
    }
    sns2 = {
      effect = "Allow",
      actions = [
        "sns:Publish",
        "sns:Subscribe"
      ],
      resources = [aws_sns_topic.etl_updates.arn]
    }
    ddb = {
      effect = "Allow",
      actions = [
        "dynamodb:*"
      ],
      resources = [aws_dynamodb_table.python-covid-etl-table.arn]
    }
  }

  allowed_triggers = {
    DailyETLTrigger = {
      service    = "events.amazonaws.com"
      source_arn = aws_cloudwatch_event_rule.daily-etl-trigger.arn
    }
  }

  tags = {
    Name      = "covid-python-etl-function"
    ManagedBy = "Terraform"
  }

  depends_on = [aws_cloudwatch_event_rule.daily-etl-trigger]
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

resource "aws_sns_topic" "etl_updates" {
  name = "etl-updates-topic"

  tags = {
    Name      = "python-covid-etl-sns-topic"
    ManagedBy = "Terraform"
  }
}

resource "aws_dynamodb_table" "python-covid-etl-table" {
  name         = "covid-data"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "date"

  attribute {
    name = "date"
    type = "S"
  }

  tags = {
    Name      = "python-covid-etl-table"
    ManagedBy = "Terraform"
  }
}

resource "aws_cloudwatch_event_rule" "daily-etl-trigger" {
  name        = "daily-python-covid-etl-trigger"
  description = "Invoke the Python COVID ETL Lambda function once daily"

  schedule_expression = "cron(0 12 * * ? *)"

  tags = {
    Name      = "python-covid-etl-trigger"
    ManagedBy = "Terraform"
  }
}

resource "aws_cloudwatch_event_target" "sns" {
  rule = aws_cloudwatch_event_rule.daily-etl-trigger.name
  arn  = module.lambda_function.lambda_function_arn

  depends_on = [module.lambda_function]
}

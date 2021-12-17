# Python ETL Cloud Challenge

Design narrative: [https://alexeversmeyer.com/post/12](https://alexeversmeyer.com/post/12)

This project centers around an event-driven AWS Lambda function, written in Python, that:

- downloads two COVID-19 data sets;
- extracts, transforms, and merges the data; and
- loads the data into a DynamoDB table.

The Lambda function is triggered by a scheduled Event Bridge event daily at 12:00 UTC. A message is published 
automatically to Simple Notification Service, alerting subscribers (me, in this case) of the success or failure of this 
process.

An EC2 instance runs an open-source copy of Redash, which performs a daily scan of the DynamoDB table shortly at 12:10 
UTC. The dashboard produced in Redash can then be refreshed for visualization of the latest data.

All infrastructure is provisioned using Terraform, and a GitHub Actions workflow updates the infrastructure with every 
push to this repository.
#!/bin/bash
# AWS Glue Job Deployment Script
# メンバー情報最新化ジョブのデプロイスクリプト

set -e

# Configuration
JOB_NAME="member-info-update-job"
SCRIPT_LOCATION="s3://m1s-glue-scripts/member_info_update.py"
ROLE_ARN="arn:aws:iam::${AWS_ACCOUNT_ID}:role/GlueServiceRole"
REGION="ap-northeast-1"

echo "Deploying AWS Glue Job: ${JOB_NAME}"

# Upload Glue script to S3
echo "Uploading Glue script to S3..."
aws s3 cp glue_jobs/member_info_update.py ${SCRIPT_LOCATION}

# Create or update Glue job
echo "Creating/updating Glue job..."
aws glue create-job \
    --name "${JOB_NAME}" \
    --role "${ROLE_ARN}" \
    --command '{
        "Name": "glueetl",
        "ScriptLocation": "'${SCRIPT_LOCATION}'",
        "PythonVersion": "3"
    }' \
    --default-arguments '{
        "--job-language": "python",
        "--job-bookmark-option": "job-bookmark-enable",
        "--enable-metrics": "",
        "--enable-continuous-cloudwatch-log": "true",
        "--enable-spark-ui": "true",
        "--spark-event-logs-path": "s3://m1s-glue-logs/sparkHistoryLogs/"
    }' \
    --max-retries 1 \
    --timeout 60 \
    --glue-version "4.0" \
    --number-of-workers 2 \
    --worker-type "G.1X" \
    --region "${REGION}" \
    2>/dev/null || \
aws glue update-job \
    --job-name "${JOB_NAME}" \
    --job-update '{
        "Role": "'${ROLE_ARN}'",
        "Command": {
            "Name": "glueetl",
            "ScriptLocation": "'${SCRIPT_LOCATION}'",
            "PythonVersion": "3"
        },
        "DefaultArguments": {
            "--job-language": "python",
            "--job-bookmark-option": "job-bookmark-enable",
            "--enable-metrics": "",
            "--enable-continuous-cloudwatch-log": "true",
            "--enable-spark-ui": "true",
            "--spark-event-logs-path": "s3://m1s-glue-logs/sparkHistoryLogs/"
        },
        "MaxRetries": 1,
        "Timeout": 60,
        "GlueVersion": "4.0",
        "NumberOfWorkers": 2,
        "WorkerType": "G.1X"
    }' \
    --region "${REGION}"

echo "Glue job deployment completed successfully!"

# Optional: Create CloudWatch schedule
if [ "${CREATE_SCHEDULE}" = "true" ]; then
    echo "Creating CloudWatch Events rule for scheduled execution..."
    aws events put-rule \
        --name "${JOB_NAME}-schedule" \
        --schedule-expression "cron(0 2 * * ? *)" \
        --description "Daily execution of member info update job at 2 AM JST" \
        --region "${REGION}"
    
    aws events put-targets \
        --rule "${JOB_NAME}-schedule" \
        --targets '[{
            "Id": "1",
            "Arn": "arn:aws:glue:'${REGION}':'${AWS_ACCOUNT_ID}':job/'${JOB_NAME}'",
            "RoleArn": "'${ROLE_ARN}'",
            "GlueParameters": {
                "SOURCE_TYPE": "s3",
                "SOURCE_PATH": "s3://m1s-data-bucket/input/members/",
                "DESTINATION_TYPE": "s3",
                "DESTINATION_PATH": "s3://m1s-data-bucket/processed/members/"
            }
        }]' \
        --region "${REGION}"
    
    echo "Schedule created successfully!"
fi

echo "Deployment process completed!"
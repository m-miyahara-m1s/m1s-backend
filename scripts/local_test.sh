#!/bin/bash
# Local testing script for the Member Info Update Job
# メンバー情報最新化ジョブのローカルテスト用スクリプト

set -e

echo "Running local tests for Member Info Update Job..."

# Create test data directory
mkdir -p /tmp/test_data/input /tmp/test_data/output

# Create sample CSV data
cat > /tmp/test_data/input/members.csv << EOF
member_id,name,email,department,join_date
M001,田中太郎,tanaka@example.com,営業部,2020-01-15
M002,佐藤花子,sato@example.com,開発部,2019-03-20
M003,鈴木次郎,suzuki@example.com,人事部,2021-05-10
M004,山田美香,,総務部,2018-08-05
M005,高橋健一,TAKAHASHI@EXAMPLE.COM,営業部,2022-02-28
EOF

echo "Sample test data created in /tmp/test_data/input/"

# Run unit tests
echo "Running unit tests..."
if command -v pytest &> /dev/null; then
    pytest tests/ -v
else
    echo "pytest not found. Please install: pip install -r tests/requirements-test.txt"
fi

# Validate Glue job syntax
echo "Validating Glue job syntax..."
python -m py_compile glue_jobs/member_info_update.py
echo "Syntax validation passed!"

# Check configuration file
echo "Validating configuration..."
if command -v python -c "import yaml" &> /dev/null; then
    python -c "import yaml; yaml.safe_load(open('config/job_config.yaml'))"
    echo "Configuration validation passed!"
else
    echo "PyYAML not found. Install with: pip install pyyaml"
fi

echo "Local testing completed successfully!"
echo "Test data location: /tmp/test_data/"
echo "To clean up: rm -rf /tmp/test_data/"
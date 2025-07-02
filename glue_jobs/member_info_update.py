#!/usr/bin/env python3
"""
AWS Glue Job: Member Information Update
メンバー情報最新化ジョブ

This job updates member information from various data sources.
"""
import sys
import boto3
import json
from datetime import datetime, timezone
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MemberInfoUpdater:
    """メンバー情報更新クラス"""
    
    def __init__(self, glue_context, spark_context, job):
        self.glue_context = glue_context
        self.spark_context = spark_context
        self.job = job
        self.current_timestamp = datetime.now(timezone.utc).isoformat()
        
    def read_source_data(self, source_config):
        """ソースデータの読み込み"""
        try:
            if source_config["type"] == "s3":
                # S3からデータを読み込み
                datasource = self.glue_context.create_dynamic_frame.from_options(
                    format_options={"multiline": False},
                    connection_type="s3",
                    format=source_config.get("format", "csv"),
                    connection_options={
                        "paths": [source_config["path"]],
                        "recurse": True
                    },
                    transformation_ctx=f"datasource_{source_config['name']}"
                )
            elif source_config["type"] == "jdbc":
                # JDBCからデータを読み込み
                datasource = self.glue_context.create_dynamic_frame.from_options(
                    connection_type="jdbc",
                    connection_options={
                        "url": source_config["url"],
                        "dbtable": source_config["table"],
                        "user": source_config["user"],
                        "password": source_config["password"]
                    },
                    transformation_ctx=f"datasource_{source_config['name']}"
                )
            else:
                raise ValueError(f"Unsupported source type: {source_config['type']}")
                
            logger.info(f"Successfully read data from {source_config['name']}")
            return datasource
            
        except Exception as e:
            logger.error(f"Failed to read data from {source_config['name']}: {str(e)}")
            raise
    
    def validate_member_data(self, df):
        """メンバーデータの検証"""
        try:
            # 必須フィールドの確認
            required_fields = ["member_id", "name", "email"]
            missing_fields = [field for field in required_fields if field not in df.columns]
            
            if missing_fields:
                raise ValueError(f"Missing required fields: {missing_fields}")
            
            # データ品質チェック
            # 重複メンバーIDの確認
            duplicate_count = df.groupBy("member_id").count().filter(F.col("count") > 1).count()
            if duplicate_count > 0:
                logger.warning(f"Found {duplicate_count} duplicate member IDs")
            
            # 空のメールアドレスの確認
            empty_email_count = df.filter(F.col("email").isNull() | (F.col("email") == "")).count()
            if empty_email_count > 0:
                logger.warning(f"Found {empty_email_count} members with empty email addresses")
            
            logger.info("Member data validation completed")
            return True
            
        except Exception as e:
            logger.error(f"Member data validation failed: {str(e)}")
            raise
    
    def transform_member_data(self, dynamic_frame):
        """メンバーデータの変換処理"""
        try:
            # DynamicFrameをDataFrameに変換
            df = dynamic_frame.toDF()
            
            # データ検証
            self.validate_member_data(df)
            
            # データ変換処理
            # 1. 名前の正規化（全角・半角統一）
            df = df.withColumn("name_normalized", 
                              F.regexp_replace(F.col("name"), "[０-９]", ""))
            
            # 2. メールアドレスの正規化（小文字化）
            df = df.withColumn("email_normalized", 
                              F.lower(F.trim(F.col("email"))))
            
            # 3. 更新日時の追加
            df = df.withColumn("updated_at", F.lit(self.current_timestamp))
            
            # 4. ステータスの設定
            df = df.withColumn("status", 
                              F.when(F.col("email_normalized").isNull() | 
                                   (F.col("email_normalized") == ""), "inactive")
                               .otherwise("active"))
            
            # 5. 不要なレコードのフィルタリング
            df = df.filter(F.col("member_id").isNotNull())
            
            logger.info(f"Transformed {df.count()} member records")
            
            # DataFrameをDynamicFrameに戻す
            return DynamicFrame.fromDF(df, self.glue_context, "transformed_members")
            
        except Exception as e:
            logger.error(f"Member data transformation failed: {str(e)}")
            raise
    
    def write_to_destination(self, dynamic_frame, destination_config):
        """変換済みデータの書き込み"""
        try:
            if destination_config["type"] == "s3":
                # S3に書き込み
                self.glue_context.write_dynamic_frame.from_options(
                    frame=dynamic_frame,
                    connection_type="s3",
                    format=destination_config.get("format", "parquet"),
                    connection_options={
                        "path": destination_config["path"],
                        "partitionKeys": destination_config.get("partition_keys", [])
                    },
                    transformation_ctx="s3_output"
                )
            elif destination_config["type"] == "jdbc":
                # JDBCに書き込み
                self.glue_context.write_dynamic_frame.from_options(
                    frame=dynamic_frame,
                    connection_type="jdbc",
                    connection_options={
                        "url": destination_config["url"],
                        "dbtable": destination_config["table"],
                        "user": destination_config["user"],
                        "password": destination_config["password"]
                    },
                    transformation_ctx="jdbc_output"
                )
            else:
                raise ValueError(f"Unsupported destination type: {destination_config['type']}")
                
            logger.info(f"Successfully wrote data to {destination_config['type']}")
            
        except Exception as e:
            logger.error(f"Failed to write data to destination: {str(e)}")
            raise
    
    def run_update_job(self, source_config, destination_config):
        """メンバー情報更新ジョブの実行"""
        try:
            logger.info("Starting member information update job")
            
            # 1. ソースデータの読み込み
            source_data = self.read_source_data(source_config)
            
            # 2. データ変換
            transformed_data = self.transform_member_data(source_data)
            
            # 3. 変換済みデータの書き込み
            self.write_to_destination(transformed_data, destination_config)
            
            logger.info("Member information update job completed successfully")
            
        except Exception as e:
            logger.error(f"Member information update job failed: {str(e)}")
            raise

def main():
    """メイン処理"""
    # Glueジョブの引数を取得
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'SOURCE_TYPE',
        'SOURCE_PATH',
        'DESTINATION_TYPE', 
        'DESTINATION_PATH'
    ])
    
    # Spark/Glueコンテキストの初期化
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    
    try:
        # ソース設定
        source_config = {
            "name": "member_source",
            "type": args['SOURCE_TYPE'],
            "path": args['SOURCE_PATH'],
            "format": "csv"
        }
        
        # 出力先設定
        destination_config = {
            "type": args['DESTINATION_TYPE'],
            "path": args['DESTINATION_PATH'],
            "format": "parquet",
            "partition_keys": ["status"]
        }
        
        # メンバー情報更新ジョブの実行
        updater = MemberInfoUpdater(glueContext, sc, job)
        updater.run_update_job(source_config, destination_config)
        
    except Exception as e:
        logger.error(f"Job execution failed: {str(e)}")
        raise
    finally:
        job.commit()

if __name__ == "__main__":
    main()
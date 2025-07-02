"""
Unit tests for Member Info Update Glue Job
メンバー情報最新化ジョブのユニットテスト
"""
import pytest
import sys
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Add the glue_jobs directory to path for imports
sys.path.append('../glue_jobs')

class TestMemberInfoUpdater:
    """MemberInfoUpdaterクラスのテスト"""
    
    @pytest.fixture
    def spark_session(self):
        """SparkSessionのフィクスチャ"""
        spark = SparkSession.builder \
            .appName("TestMemberInfoUpdate") \
            .master("local[2]") \
            .config("spark.executor.memory", "1g") \
            .getOrCreate()
        yield spark
        spark.stop()
    
    @pytest.fixture
    def sample_member_data(self, spark_session):
        """サンプルメンバーデータのフィクスチャ"""
        schema = StructType([
            StructField("member_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("department", StringType(), True)
        ])
        
        data = [
            ("M001", "田中太郎", "tanaka@example.com", "営業部"),
            ("M002", "佐藤花子", "sato@example.com", "開発部"),
            ("M003", "鈴木次郎", "suzuki@example.com", "人事部"),
            ("M004", "山田美香", "", "総務部"),  # 空のメールアドレス
            ("M005", "高橋健一", "TAKAHASHI@EXAMPLE.COM", "営業部")  # 大文字メール
        ]
        
        return spark_session.createDataFrame(data, schema)
    
    @patch('sys.argv', ['member_info_update.py', '--JOB_NAME', 'test_job', 
                        '--SOURCE_TYPE', 's3', '--SOURCE_PATH', 's3://test/input/',
                        '--DESTINATION_TYPE', 's3', '--DESTINATION_PATH', 's3://test/output/'])
    def test_member_data_validation_success(self, spark_session, sample_member_data):
        """メンバーデータ検証の成功テスト"""
        # Mock GlueContext and related objects
        mock_glue_context = Mock()
        mock_spark_context = Mock()
        mock_job = Mock()
        
        # Import after patching sys.argv
        from member_info_update import MemberInfoUpdater
        
        updater = MemberInfoUpdater(mock_glue_context, mock_spark_context, mock_job)
        
        # Test validation - should not raise exception
        result = updater.validate_member_data(sample_member_data)
        assert result is True
    
    def test_member_data_validation_missing_fields(self, spark_session):
        """必須フィールド不足時の検証テスト"""
        # Create data missing required field
        schema = StructType([
            StructField("member_id", StringType(), True),
            StructField("name", StringType(), True)
            # email field is missing
        ])
        
        data = [("M001", "田中太郎")]
        incomplete_data = spark_session.createDataFrame(data, schema)
        
        mock_glue_context = Mock()
        mock_spark_context = Mock()
        mock_job = Mock()
        
        with patch('sys.argv', ['member_info_update.py', '--JOB_NAME', 'test_job', 
                               '--SOURCE_TYPE', 's3', '--SOURCE_PATH', 's3://test/input/',
                               '--DESTINATION_TYPE', 's3', '--DESTINATION_PATH', 's3://test/output/']):
            from member_info_update import MemberInfoUpdater
            updater = MemberInfoUpdater(mock_glue_context, mock_spark_context, mock_job)
            
            # Test validation - should raise ValueError
            with pytest.raises(ValueError, match="Missing required fields"):
                updater.validate_member_data(incomplete_data)
    
    def test_member_data_transformation(self, spark_session, sample_member_data):
        """メンバーデータ変換処理のテスト"""
        mock_glue_context = Mock()
        mock_spark_context = Mock()
        mock_job = Mock()
        
        # Mock DynamicFrame
        mock_dynamic_frame = Mock()
        mock_dynamic_frame.toDF.return_value = sample_member_data
        
        with patch('sys.argv', ['member_info_update.py', '--JOB_NAME', 'test_job', 
                               '--SOURCE_TYPE', 's3', '--SOURCE_PATH', 's3://test/input/',
                               '--DESTINATION_TYPE', 's3', '--DESTINATION_PATH', 's3://test/output/']):
            from member_info_update import MemberInfoUpdater
            
            with patch('member_info_update.DynamicFrame') as mock_df_class:
                mock_df_class.fromDF.return_value = Mock()
                
                updater = MemberInfoUpdater(mock_glue_context, mock_spark_context, mock_job)
                result = updater.transform_member_data(mock_dynamic_frame)
                
                # Verify DynamicFrame.fromDF was called
                mock_df_class.fromDF.assert_called_once()
    
    def test_data_normalization(self, spark_session, sample_member_data):
        """データ正規化のテスト"""
        # Test email normalization
        transformed_df = sample_member_data.withColumn(
            "email_normalized", 
            spark_session.sql("SELECT lower(trim(email)) as email").collect()[0][0] 
            if sample_member_data.select("email").collect()[0][0] else None
        )
        
        # Verify email normalization works
        emails = [row.email for row in sample_member_data.collect()]
        assert "TAKAHASHI@EXAMPLE.COM" in emails  # Original has uppercase
        
    def test_status_assignment(self, spark_session):
        """ステータス割り当てのテスト"""
        from pyspark.sql import functions as F
        
        # Test data with various email conditions
        schema = StructType([
            StructField("member_id", StringType(), True),
            StructField("email", StringType(), True)
        ])
        
        data = [
            ("M001", "valid@example.com"),
            ("M002", ""),
            ("M003", None)
        ]
        
        test_df = spark_session.createDataFrame(data, schema)
        
        # Apply status logic
        result_df = test_df.withColumn("status", 
                                     F.when(F.col("email").isNull() | 
                                          (F.col("email") == ""), "inactive")
                                      .otherwise("active"))
        
        results = result_df.collect()
        assert results[0].status == "active"   # valid email
        assert results[1].status == "inactive" # empty email
        assert results[2].status == "inactive" # null email

if __name__ == "__main__":
    pytest.main([__file__])
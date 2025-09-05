#!/usr/bin/env python3
"""
Test script for PySpark Deduplication Job

This script creates sample data and tests the deduplication functionality.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import tempfile
import os
from pyspark_dedup_job import PySparkDedupJob


def create_test_spark_session():
    """Create a Spark session for testing."""
    return SparkSession.builder \
        .appName("Test-PySpark-Dedup-Job") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .master("local[*]") \
        .getOrCreate()


def create_sample_data(spark):
    """Create sample data with duplicates for testing."""
    
    # Define schema
    schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("email", StringType(), True),
        StructField("name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("created_date", StringType(), True)
    ])
    
    # Sample data with duplicates
    data = [
        (1, "john@email.com", "John Doe", "New York", "2024-01-01"),
        (2, "jane@email.com", "Jane Smith", "Los Angeles", "2024-01-02"),
        (1, "john@email.com", "John Doe", "New York", "2024-01-01"),  # Duplicate
        (3, "bob@email.com", "Bob Johnson", "Chicago", "2024-01-03"),
        (2, "jane@email.com", "Jane Smith", "Los Angeles", "2024-01-02"),  # Duplicate
        (4, "alice@email.com", "Alice Brown", "Houston", "2024-01-04"),
        (1, "john@email.com", "John Doe Updated", "New York", "2024-01-05"),  # Duplicate with updated info
        (5, "charlie@email.com", "Charlie Wilson", "Phoenix", "2024-01-05"),
    ]
    
    df = spark.createDataFrame(data, schema)
    return df


def test_simple_deduplication():
    """Test simple deduplication without ordering."""
    print("\n" + "="*60)
    print("TESTING SIMPLE DEDUPLICATION")
    print("="*60)
    
    spark = create_test_spark_session()
    
    try:
        # Create sample data
        sample_df = create_sample_data(spark)
        print(f"Created sample data with {sample_df.count()} records")
        
        # Show original data
        print("\nOriginal data:")
        sample_df.show()
        
        # Initialize dedup job
        dedup_job = PySparkDedupJob(spark)
        
        # Perform simple deduplication
        deduped_df = dedup_job.deduplicate_data(
            sample_df, 
            dedup_columns=["customer_id", "email"]
        )
        
        # Show deduplicated data
        print("\nDeduplicated data:")
        deduped_df.show()
        
        print(f"✅ Simple deduplication test completed successfully!")
        
    finally:
        spark.stop()


def test_ordered_deduplication():
    """Test ordered deduplication with timestamp ordering."""
    print("\n" + "="*60)
    print("TESTING ORDERED DEDUPLICATION")
    print("="*60)
    
    spark = create_test_spark_session()
    
    try:
        # Create sample data with timestamps
        sample_df = create_sample_data(spark)
        print(f"Created sample data with {sample_df.count()} records")
        
        # Show original data
        print("\nOriginal data:")
        sample_df.show()
        
        # Initialize dedup job
        dedup_job = PySparkDedupJob(spark)
        
        # Perform ordered deduplication (keep last based on created_date)
        deduped_df = dedup_job.deduplicate_data(
            sample_df, 
            dedup_columns=["customer_id", "email"],
            order_by_column="created_date",
            keep="last"
        )
        
        # Show deduplicated data
        print("\nDeduplicated data (keeping last by created_date):")
        deduped_df.orderBy("customer_id").show()
        
        print(f"✅ Ordered deduplication test completed successfully!")
        
    finally:
        spark.stop()


def test_file_based_deduplication():
    """Test file-based deduplication (CSV to Parquet)."""
    print("\n" + "="*60)
    print("TESTING FILE-BASED DEDUPLICATION")
    print("="*60)
    
    spark = create_test_spark_session()
    
    try:
        # Create temporary directories
        with tempfile.TemporaryDirectory() as temp_dir:
            source_path = os.path.join(temp_dir, "source.csv")
            target_path = os.path.join(temp_dir, "target.parquet")
            
            # Create sample data and save as CSV
            sample_df = create_sample_data(spark)
            sample_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(source_path)
            
            print(f"Created source CSV file at: {source_path}")
            
            # Initialize dedup job
            dedup_job = PySparkDedupJob(spark)
            
            # Run complete deduplication job
            dedup_job.run_dedup_job(
                source_table=source_path,
                target_table=target_path,
                dedup_columns=["customer_id", "email"],
                source_format="csv",
                target_format="parquet",
                order_by_column="created_date",
                keep="last"
            )
            
            # Read and display results
            result_df = spark.read.parquet(target_path)
            print(f"\nFinal deduplicated data ({result_df.count()} records):")
            result_df.orderBy("customer_id").show()
            
            print(f"✅ File-based deduplication test completed successfully!")
            
    finally:
        spark.stop()


def main():
    """Run all tests."""
    print("🚀 Starting PySpark Deduplication Job Tests")
    
    try:
        # Run tests
        test_simple_deduplication()
        test_ordered_deduplication()
        test_file_based_deduplication()
        
        print("\n" + "="*60)
        print("🎉 ALL TESTS COMPLETED SUCCESSFULLY!")
        print("="*60)
        
    except Exception as e:
        print(f"\n❌ Test failed with error: {str(e)}")
        raise


if __name__ == "__main__":
    main()

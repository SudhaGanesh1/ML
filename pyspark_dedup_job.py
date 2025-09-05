#!/usr/bin/env python3
"""
PySpark Deduplication Job

This script reads data from a source table, performs deduplication based on specified columns,
and writes the deduplicated data to a target table.

Usage:
    spark-submit pyspark_dedup_job.py --source-table <source_table> --target-table <target_table> [options]

Author: Codegen
"""

import argparse
import logging
from typing import List, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, row_number, max as spark_max, desc
from pyspark.sql.window import Window


class PySparkDedupJob:
    """PySpark job for deduplicating data from source to target table."""
    
    def __init__(self, spark_session: SparkSession):
        """Initialize the deduplication job with Spark session."""
        self.spark = spark_session
        self.logger = self._setup_logging()
    
    def _setup_logging(self) -> logging.Logger:
        """Set up logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)
    
    def read_source_table(self, source_table: str, source_format: str = "delta") -> DataFrame:
        """
        Read data from the source table.
        
        Args:
            source_table: Name or path of the source table
            source_format: Format of the source table (delta, parquet, csv, etc.)
            
        Returns:
            DataFrame containing source data
        """
        self.logger.info(f"Reading source table: {source_table}")
        
        try:
            if source_format.lower() == "delta":
                df = self.spark.read.format("delta").table(source_table)
            elif source_format.lower() == "parquet":
                df = self.spark.read.parquet(source_table)
            elif source_format.lower() == "csv":
                df = self.spark.read.option("header", "true").csv(source_table)
            else:
                df = self.spark.read.format(source_format).table(source_table)
            
            record_count = df.count()
            self.logger.info(f"Successfully read {record_count:,} records from {source_table}")
            return df
            
        except Exception as e:
            self.logger.error(f"Error reading source table {source_table}: {str(e)}")
            raise
    
    def deduplicate_data(
        self, 
        df: DataFrame, 
        dedup_columns: List[str], 
        order_by_column: Optional[str] = None,
        keep: str = "last"
    ) -> DataFrame:
        """
        Deduplicate data based on specified columns.
        
        Args:
            df: Input DataFrame
            dedup_columns: List of columns to use for deduplication
            order_by_column: Column to use for ordering when determining which record to keep
            keep: Which record to keep ('first' or 'last')
            
        Returns:
            Deduplicated DataFrame
        """
        self.logger.info(f"Starting deduplication on columns: {dedup_columns}")
        
        original_count = df.count()
        self.logger.info(f"Original record count: {original_count:,}")
        
        try:
            if order_by_column:
                # Use window function with ordering
                if keep == "last":
                    window_spec = Window.partitionBy(*dedup_columns).orderBy(desc(order_by_column))
                else:
                    window_spec = Window.partitionBy(*dedup_columns).orderBy(col(order_by_column))
                
                deduped_df = df.withColumn("row_num", row_number().over(window_spec)) \
                              .filter(col("row_num") == 1) \
                              .drop("row_num")
            else:
                # Simple deduplication without ordering
                deduped_df = df.dropDuplicates(dedup_columns)
            
            deduped_count = deduped_df.count()
            duplicates_removed = original_count - deduped_count
            
            self.logger.info(f"Deduplication completed:")
            self.logger.info(f"  - Original records: {original_count:,}")
            self.logger.info(f"  - Deduplicated records: {deduped_count:,}")
            self.logger.info(f"  - Duplicates removed: {duplicates_removed:,}")
            self.logger.info(f"  - Duplicate percentage: {(duplicates_removed/original_count)*100:.2f}%")
            
            return deduped_df
            
        except Exception as e:
            self.logger.error(f"Error during deduplication: {str(e)}")
            raise
    
    def write_target_table(
        self, 
        df: DataFrame, 
        target_table: str, 
        target_format: str = "delta",
        write_mode: str = "overwrite",
        partition_columns: Optional[List[str]] = None
    ) -> None:
        """
        Write deduplicated data to target table.
        
        Args:
            df: Deduplicated DataFrame
            target_table: Name or path of the target table
            target_format: Format of the target table (delta, parquet, csv, etc.)
            write_mode: Write mode (overwrite, append, etc.)
            partition_columns: List of columns to partition by
        """
        self.logger.info(f"Writing to target table: {target_table}")
        
        try:
            writer = df.write.mode(write_mode)
            
            if partition_columns:
                writer = writer.partitionBy(*partition_columns)
                self.logger.info(f"Partitioning by columns: {partition_columns}")
            
            if target_format.lower() == "delta":
                writer.format("delta").saveAsTable(target_table)
            elif target_format.lower() == "parquet":
                writer.parquet(target_table)
            elif target_format.lower() == "csv":
                writer.option("header", "true").csv(target_table)
            else:
                writer.format(target_format).saveAsTable(target_table)
            
            record_count = df.count()
            self.logger.info(f"Successfully wrote {record_count:,} records to {target_table}")
            
        except Exception as e:
            self.logger.error(f"Error writing to target table {target_table}: {str(e)}")
            raise
    
    def run_dedup_job(
        self,
        source_table: str,
        target_table: str,
        dedup_columns: List[str],
        source_format: str = "delta",
        target_format: str = "delta",
        order_by_column: Optional[str] = None,
        keep: str = "last",
        write_mode: str = "overwrite",
        partition_columns: Optional[List[str]] = None
    ) -> None:
        """
        Run the complete deduplication job.
        
        Args:
            source_table: Source table name/path
            target_table: Target table name/path
            dedup_columns: Columns to deduplicate on
            source_format: Source table format
            target_format: Target table format
            order_by_column: Column for ordering during deduplication
            keep: Which duplicate to keep ('first' or 'last')
            write_mode: Write mode for target table
            partition_columns: Columns to partition target table by
        """
        self.logger.info("=" * 60)
        self.logger.info("STARTING PYSPARK DEDUPLICATION JOB")
        self.logger.info("=" * 60)
        
        try:
            # Step 1: Read source data
            source_df = self.read_source_table(source_table, source_format)
            
            # Step 2: Perform deduplication
            deduped_df = self.deduplicate_data(
                source_df, 
                dedup_columns, 
                order_by_column, 
                keep
            )
            
            # Step 3: Write to target table
            self.write_target_table(
                deduped_df, 
                target_table, 
                target_format, 
                write_mode, 
                partition_columns
            )
            
            self.logger.info("=" * 60)
            self.logger.info("DEDUPLICATION JOB COMPLETED SUCCESSFULLY")
            self.logger.info("=" * 60)
            
        except Exception as e:
            self.logger.error(f"Job failed with error: {str(e)}")
            raise


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="PySpark Deduplication Job")
    
    # Required arguments
    parser.add_argument("--source-table", required=True, 
                       help="Source table name or path")
    parser.add_argument("--target-table", required=True,
                       help="Target table name or path")
    parser.add_argument("--dedup-columns", required=True, nargs='+',
                       help="Columns to use for deduplication")
    
    # Optional arguments
    parser.add_argument("--source-format", default="delta",
                       help="Source table format (default: delta)")
    parser.add_argument("--target-format", default="delta",
                       help="Target table format (default: delta)")
    parser.add_argument("--order-by-column", 
                       help="Column to order by when deduplicating")
    parser.add_argument("--keep", choices=["first", "last"], default="last",
                       help="Which duplicate to keep (default: last)")
    parser.add_argument("--write-mode", default="overwrite",
                       help="Write mode for target table (default: overwrite)")
    parser.add_argument("--partition-columns", nargs='+',
                       help="Columns to partition target table by")
    parser.add_argument("--app-name", default="PySpark-Dedup-Job",
                       help="Spark application name")
    
    return parser.parse_args()


def create_spark_session(app_name: str) -> SparkSession:
    """Create and configure Spark session."""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


def main():
    """Main function to run the deduplication job."""
    args = parse_arguments()
    
    # Create Spark session
    spark = create_spark_session(args.app_name)
    
    try:
        # Initialize and run the dedup job
        dedup_job = PySparkDedupJob(spark)
        
        dedup_job.run_dedup_job(
            source_table=args.source_table,
            target_table=args.target_table,
            dedup_columns=args.dedup_columns,
            source_format=args.source_format,
            target_format=args.target_format,
            order_by_column=args.order_by_column,
            keep=args.keep,
            write_mode=args.write_mode,
            partition_columns=args.partition_columns
        )
        
    finally:
        # Stop Spark session
        spark.stop()


if __name__ == "__main__":
    main()

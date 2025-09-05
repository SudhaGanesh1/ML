# PySpark Deduplication Job

A comprehensive PySpark job for deduplicating data from source tables to target tables with flexible configuration options.

## Features

- ✅ **Multiple Format Support**: Delta, Parquet, CSV, and other Spark-supported formats
- ✅ **Flexible Deduplication**: Simple column-based or advanced ordering-based deduplication
- ✅ **Partitioning Support**: Automatic partitioning of target tables
- ✅ **Comprehensive Logging**: Detailed logging with duplicate statistics
- ✅ **Error Handling**: Robust error handling and recovery
- ✅ **Command Line Interface**: Easy-to-use CLI with extensive options

## Quick Start

### Basic Usage

```bash
spark-submit pyspark_dedup_job.py \
  --source-table "raw.customers" \
  --target-table "clean.customers_deduped" \
  --dedup-columns customer_id email
```

### Advanced Usage with Ordering

```bash
spark-submit pyspark_dedup_job.py \
  --source-table "raw.transactions" \
  --target-table "clean.transactions_deduped" \
  --dedup-columns transaction_id \
  --order-by-column created_timestamp \
  --keep last \
  --partition-columns date_partition
```

## Command Line Arguments

### Required Arguments
- `--source-table`: Source table name or path
- `--target-table`: Target table name or path  
- `--dedup-columns`: Space-separated list of columns to deduplicate on

### Optional Arguments
- `--source-format`: Source table format (default: delta)
- `--target-format`: Target table format (default: delta)
- `--order-by-column`: Column to order by when determining which record to keep
- `--keep`: Which duplicate to keep - 'first' or 'last' (default: last)
- `--write-mode`: Write mode for target table (default: overwrite)
- `--partition-columns`: Space-separated list of columns to partition by
- `--app-name`: Spark application name (default: PySpark-Dedup-Job)

## Usage Examples

### 1. Basic Customer Deduplication
Remove duplicate customers based on customer_id and email:

```bash
spark-submit pyspark_dedup_job.py \
  --source-table "raw.customers" \
  --target-table "clean.customers_deduped" \
  --dedup-columns customer_id email
```

### 2. Transaction Deduplication with Latest Record
Keep the latest transaction for each transaction_id:

```bash
spark-submit pyspark_dedup_job.py \
  --source-table "raw.transactions" \
  --target-table "clean.transactions_deduped" \
  --dedup-columns transaction_id \
  --order-by-column created_timestamp \
  --keep last \
  --partition-columns date_partition
```

### 3. CSV to Parquet Deduplication
Convert and deduplicate from CSV to Parquet:

```bash
spark-submit pyspark_dedup_job.py \
  --source-table "/path/to/source/data.csv" \
  --target-table "/path/to/target/data.parquet" \
  --dedup-columns id name \
  --source-format csv \
  --target-format parquet
```

### 4. Complex Multi-Column Deduplication
Deduplicate user events with multiple columns and partitioning:

```bash
spark-submit pyspark_dedup_job.py \
  --source-table "raw.user_events" \
  --target-table "clean.user_events_deduped" \
  --dedup-columns user_id event_type event_date \
  --order-by-column event_timestamp \
  --keep last \
  --partition-columns event_date event_type
```

## Deduplication Strategies

### 1. Simple Deduplication
Uses Spark's `dropDuplicates()` function:
- Fast and efficient for basic deduplication
- No ordering guarantees on which record is kept
- Best for cases where all duplicate records are identical

### 2. Ordered Deduplication  
Uses window functions with `row_number()`:
- Allows control over which duplicate record to keep
- Can specify ordering column (e.g., timestamp, id)
- Choose to keep 'first' or 'last' record based on ordering
- More resource-intensive but provides deterministic results

## Supported Formats

| Format | Read | Write | Notes |
|--------|------|-------|-------|
| Delta | ✅ | ✅ | Recommended for ACID transactions |
| Parquet | ✅ | ✅ | Efficient columnar format |
| CSV | ✅ | ✅ | Includes header handling |
| JSON | ✅ | ✅ | Via generic format reader |
| Avro | ✅ | ✅ | Via generic format reader |

## Performance Considerations

### Optimization Tips
1. **Partitioning**: Use appropriate partition columns to improve query performance
2. **Column Selection**: Only include necessary columns in deduplication logic
3. **Caching**: For multiple operations on the same dataset, consider caching
4. **Cluster Resources**: Ensure adequate memory and cores for large datasets

### Spark Configuration
The job includes optimized Spark configurations:
- Adaptive Query Execution (AQE) enabled
- Automatic partition coalescing
- Delta Lake extensions (if using Delta format)

## Monitoring and Logging

The job provides comprehensive logging including:
- Record counts (original vs. deduplicated)
- Duplicate statistics and percentages
- Processing time for each step
- Error details with stack traces

### Sample Log Output
```
2024-01-15 10:30:15 - INFO - Reading source table: raw.customers
2024-01-15 10:30:20 - INFO - Successfully read 1,000,000 records from raw.customers
2024-01-15 10:30:25 - INFO - Starting deduplication on columns: ['customer_id', 'email']
2024-01-15 10:30:35 - INFO - Deduplication completed:
2024-01-15 10:30:35 - INFO -   - Original records: 1,000,000
2024-01-15 10:30:35 - INFO -   - Deduplicated records: 950,000
2024-01-15 10:30:35 - INFO -   - Duplicates removed: 50,000
2024-01-15 10:30:35 - INFO -   - Duplicate percentage: 5.00%
```

## Error Handling

The job includes robust error handling for common scenarios:
- Invalid table names or paths
- Missing columns in deduplication logic
- Format-specific read/write errors
- Insufficient cluster resources
- Network connectivity issues

## Requirements

- Apache Spark 3.0+
- Python 3.7+
- Delta Lake (if using Delta format)
- Appropriate cluster permissions for source/target locations

## Configuration File

See `dedup_config.yaml` for example configurations that can be adapted for your use cases.

## Contributing

Feel free to submit issues and enhancement requests!

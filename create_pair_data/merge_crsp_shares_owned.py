# python '18_shares_owned/create_pair_data/merge_crsp_shares_owned.py'
# =============================================================================
# Section 1: Import Libraries
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, concat, lit, last, col, hash, expr, min
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.window import Window
from pyspark.ml.feature import StringIndexer
from pyspark.sql import functions as F
import time
from pyspark.sql import DataFrame
from pyspark.sql import Column

# =============================================================================
# Section 2: Create functions
# =============================================================================


def forward_fill_columns(df, columns, windowSpec):
    """
    Forward fills specified columns in a DataFrame based on a given window specification.

    Parameters:
    - df: The DataFrame to process.
    - columns: A list of column names to forward fill.
    - windowSpec: The window specification to use for forward filling.

    Returns:
    - A DataFrame with the specified columns forward filled.
    """
    for column in columns:
        filled_column_name = f"{column}_filled"
        df = df.withColumn(filled_column_name, last(df[column], ignorenulls=True).over(windowSpec))
        df = df.drop(column).withColumnRenamed(filled_column_name, column)
    return df


# Didn't use this function but might use it.
def reorder_columns(dataset: DataFrame, column_name: str, target_column_name: str) -> DataFrame:
    """
    Reorders columns in a Spark DataFrame by placing the specified column
    directly after the target column.

    Parameters:
    - dataset: The input DataFrame.
    - column_name: The name of the column to move.
    - target_column_name: The name of the column after which the specified column should be placed.

    Returns:
    - DataFrame: A new DataFrame with columns reordered as specified.
    """
    # Ensure the column to move and the target column exist in the DataFrame
    if column_name not in dataset.columns:
        raise ValueError(f"Column '{column_name}' does not exist in the DataFrame.")
    if target_column_name not in dataset.columns:
        raise ValueError(f"Target column '{target_column_name}' does not exist in the DataFrame.")

    # Create a list of columns excluding the column to move
    columns_list = [c for c in dataset.columns if c != column_name]

    # Find the index of the target column
    target_index = columns_list.index(target_column_name) + 1

    # Insert the column to move right after the target column
    columns_list.insert(target_index, column_name)

    # Select columns in the new order
    reordered_dataset = dataset.select(*columns_list)

    return reordered_dataset

# =============================================================================
# Section 3: Initialize Spark Session
# =============================================================================


spark = SparkSession.builder \
    .appName("DataMergeExample") \
    .master("local[*]") \
    .config("spark.driver.memory", "12g") \
    .config("spark.executor.memory", "12g") \
    .config("spark.executor.memoryOverhead", "2g") \
    .config("spark.hadoop.hadoop.home.dir", "C:\\hadoop") \
    .config("spark.hadoop.hadoop.bin.path", "C:\\hadoop\\bin") \
    .getOrCreate()

# =============================================================================
# Section 3: Import Data
# =============================================================================

# Start timer
start_time = time.time()

# Load shares_owned data
shares_owned = spark.read.csv('17_data_new/shares_owned', header=True, inferSchema=True)
shares_owned = shares_owned.withColumn('date_sample', to_date(shares_owned['date_sample'], 'MM-dd-yyyy'))
shares_owned = shares_owned.withColumn('pair_id', hash(concat(shares_owned['cusips'], shares_owned['cik_owner'])).cast(IntegerType()))
shares_owned = shares_owned.withColumnRenamed('cik', 'cik_company')  # avoids confusion

# Drop column
shares_owned = shares_owned.drop('file_path')  # takes too much memory.

# Load CRSP data and convert YYYYMMDD to date type
crsp_data = spark.read.csv('17_data_new/short_crsp_data.csv', header=True, inferSchema=True)
crsp_data = crsp_data.withColumn("YYYYMMDD", to_date(crsp_data["YYYYMMDD"], "yyyyMMdd"))

# Remove cik_owner not present in CRSP (reduces datasize)
distinct_cik = crsp_data.select("CIK").distinct()
shares_owned = shares_owned.join(distinct_cik, shares_owned.cik_owner == distinct_cik.CIK, 'semi')

# =============================================================================
# Section 4: Cross Join Between a Calendar and shares_owned
# =============================================================================

# Calculate the minimum date_sample for each pair_id
min_dates = shares_owned.groupBy('pair_id').agg(min('date_sample').alias('min_date_sample'))

# Create calendar DataFrame
calendar_start = '1990-01-01'
calendar_end = '2024-12-31'
calendar_df = spark.range(start=spark.sql(f"SELECT cast(datediff('{calendar_end}', '{calendar_start}') as bigint)").collect()[0][0] + 1).selectExpr("cast(id as int) as id").selectExpr(f"date_add(to_date('{calendar_start}'), id) as date_sample")

# Perform the modified cross join
# First, cross join calendar_df with distinct pair_id and their min_date_sample
# Then, filter out dates in calendar_df that are before each pair_id's min_date_sample
modified_cross_join = calendar_df.crossJoin(min_dates).where("date_sample >= min_date_sample")

# Join the modified cross join result with shares_owned to get the final merged data
merged_data = modified_cross_join.join(shares_owned, on=['date_sample', 'pair_id'], how='left')

# =============================================================================
# Section 5: Forward Fill the Data coming from shares_owned
# =============================================================================

# Forward fill shares_owned
windowSpec = Window.partitionBy('pair_id').orderBy('date_sample').rowsBetween(Window.unboundedPreceding, 0)

columns_to_fill = ['cusips', 'cik_owner', 'company', 'owner', 'cik_company', 'file_type',
                   'date_issue', 'date_transaction',
                   'shares_agg', 'shares_agg',
                   'shares_sole_vote', 'shares_shared_vote',
                   'shares_sole_dispositive','shares_shared_dispositive',
                   'shares_percentage']
merged_data = forward_fill_columns(merged_data, columns_to_fill, windowSpec)

# =============================================================================
# Section 6: Join CRSP and the shares_owned Spread over a Calendar
# =============================================================================

# -----------------------------------------------------------------------------
# Subsection 6.1: Join CRSP for the Subsidiary on CUSIP8
# -----------------------------------------------------------------------------

# Slice the first 8 characters of the 'cusips' column
merged_data = merged_data.withColumn('cusips_sliced', F.expr("substring(cusips, 1, 8)"))

# Perform the inner join using the sliced 'cusips' and 'YYYYMMDD'
final_data_1 = merged_data.join(
    crsp_data,
    (merged_data.date_sample == crsp_data.YYYYMMDD) & (merged_data.cusips_sliced == crsp_data.CUSIP),
    'inner'
)

# Drop redundant columns
final_data_1 = final_data_1.drop('YYYYMMDD')
final_data_1 = final_data_1.drop('CIK')
final_data_1 = final_data_1.drop('CUSIP')
final_data_1 = final_data_1.drop('cusips_sliced')

# -----------------------------------------------------------------------------
# Subsection 6.2: Join CRSP for the Subsidiary on CUSIP6 and Concatenate with CUSIP8 data
# -----------------------------------------------------------------------------

# Keep only pairs not already present in crsp with the cusip 8
merged_data_anti = merged_data.join(final_data_1, on='pair_id', how='left_anti')

# Slice the first 6 characters of the 'cusips' column
merged_data_anti = merged_data_anti.withColumn('cusips_sliced', F.expr("substring(cusips, 1, 6)"))

# Perform the inner join using the sliced 'cusips' and 'YYYYMMDD'
final_data_2 = merged_data_anti.join(
    crsp_data,
    (merged_data_anti.date_sample == crsp_data.YYYYMMDD) & (merged_data_anti.cusips_sliced == crsp_data.CUSIP6),
    'inner'
)

# Drop redundant columns
final_data_2 = final_data_2.drop('YYYYMMDD')
final_data_2 = final_data_2.drop('CIK')
final_data_2 = final_data_2.drop('CUSIP')
final_data_2 = final_data_2.drop('cusips_sliced')

# Concatenate final_data_1 and final_data_2
final_data = final_data_1.unionByName(final_data_2)

# -----------------------------------------------------------------------------
# Subsection 6.3: Join CRSP for the Owner (Parent)
# -----------------------------------------------------------------------------

# Iterate over each column in crsp_data and rename it by appending '_o'
for column_name in crsp_data.columns:
    crsp_data = crsp_data.withColumnRenamed(column_name, column_name + '_o')

# Second Inner Join based on 'cik_owner' and 'CIK'
final_data = final_data.join(
    crsp_data,
    (final_data.date_sample == crsp_data.YYYYMMDD_o) & (final_data.cik_owner == crsp_data.CIK_o),
    'inner'
)

# Drop redundant columns
final_data = final_data.drop('YYYYMMDD_o')
final_data = final_data.drop('CIK_o')

# =============================================================================
# Section 7: Clean & Export
# =============================================================================

# Modify the date_issue format
final_data = final_data.withColumn('date_issue', to_date(final_data['date_issue'], 'MM-dd-yyyy'))

# Order data
final_data = final_data.orderBy(col("pair_id"), col("date_sample"))

# Save the merged and forward-filled data to CSV
final_data.write.mode('overwrite').csv('17_data_new/final_data_filled', header=True)

# End timer and print duration
end_time = (time.time() - start_time) / 60  # Convert to minutes
print(f"It took {end_time} minutes.")


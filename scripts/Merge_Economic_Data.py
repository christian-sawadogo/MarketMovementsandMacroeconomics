from pyspark.sql.functions import split, expr
from pyspark.sql.functions import col, monotonically_increasing_id, row_number
from pyspark.sql.window import Window


def process_file(spark, file_path, skip_lines, remove_lines, drop_columns=[], drop_last_column=False):
    # Read the file as a Resilient Distributed Dataset (RDD)
    rdd = spark.read.text(file_path).rdd

    # Skip initial lines that might contain metadata
    data_rdd = rdd.zipWithIndex().filter(lambda x: x[1] > skip_lines).map(lambda x: x[0])

    # Extract headers from the first data line
    header = data_rdd.first()[0].split(";")

    # Create unique column names to avoid duplication
    new_header = [f"{col_name}_{i}" if i > 0 and col_name == header[0] else col_name for i, col_name in
                  enumerate(header)]

    # Parse remaining lines as CSV
    data_rdd = data_rdd.filter(lambda row: row != header).map(lambda row: row[0].split(";"))

    # Convert RDD to DataFrame with unique column names
    df = data_rdd.toDF(new_header)

    # Add a sequential index column to the DataFrame
    df = df.withColumn("index", monotonically_increasing_id())

    # Window specification to order by the index
    windowSpec = Window.orderBy(col("index"))

    # Add a row number based on the index
    df = df.withColumn("row_num", row_number().over(windowSpec))

    # Count total rows in DataFrame
    total_rows = df.count()

    # Filter rows based on specified number of lines to skip at the start and end
    df_filtered = df.filter((col("row_num") > 3) & (col("row_num") <= (total_rows - remove_lines)))

    # Drop auxiliary columns 'index' and 'row_num'
    df_filtered = df_filtered.drop("index", "row_num")

    # Drop specified columns
    for i in drop_columns:
        df_filtered = df_filtered.drop(i)

    # Condition to filter out rows with null or empty strings in non-first columns
    rest_columns = df_filtered.columns[1:]
    condition = None
    for column in rest_columns:
        current_condition = (col(column).isNotNull()) & (col(column) != "")
        condition = current_condition if condition is None else (condition | current_condition)

    # Apply the filter condition to the DataFrame
    df_final = df_filtered.filter(condition)

    # Optionally drop the last column
    if drop_last_column:
        last_column_name = df_final.columns[-1]
        df_final = df_final.drop(last_column_name)

    # Convert to Pandas DataFrame for transposition
    pandas_df = df_final.toPandas()

    # Transpose the DataFrame
    transposed_df = pandas_df.set_index(pandas_df.columns[0]).T

    # Ensure unique column names after transposition
    transposed_df.columns = [f"{col}_{i}" for i, col in enumerate(transposed_df.columns)]

    # Convert back to PySpark DataFrame
    spark_df_transposed = spark.createDataFrame(transposed_df.reset_index())

    return spark_df_transposed


def rename_duplicated_columns(df, suffix):
    new_column_names = []
    seen = set()
    for c in df.columns:
        if c in seen:
            new_column_names.append(c + suffix)
        else:
            new_column_names.append(c)
            seen.add(c)
    return df.toDF(*new_column_names)


def clean_column_name(column_name):
    column_name = column_name.strip("�__")
    for char in [" ", ">", ",", "(", ")"]:
        column_name = column_name.replace(char, "_")
    column_name = column_name.replace("__", "_")
    return column_name


def split_index(df):
    split_col = split(df['index'], '-')
    df = df.withColumn('Year', split_col.getItem(0))
    df = df.withColumn('Quarter', split_col.getItem(1))
    return df.drop('index')


def merge_func(spark, p1, p2, p3):
    df_1 = process_file(spark, p1, 4, 4, drop_columns=["Time period_1"], drop_last_column=False)
    df_2 = process_file(spark, p2, 3, 2, drop_columns=["Time period_1", "Time period_2"], drop_last_column=True)
    df_3 = process_file(spark, p3, 3, 2, drop_columns=["Time period_1", "Time period_2"], drop_last_column=True)

    df_1 = split_index(df_1)
    df_2 = split_index(df_2)
    df_3 = split_index(df_3)

    df_2 = rename_duplicated_columns(df_2, '_df2')
    df_3 = rename_duplicated_columns(df_3, '_df3')

    # Merge DataFrames based on 'Year' and 'Quarter'
    merged_df = df_1.join(df_2, ['Year', 'Quarter'], "inner") \
        .join(df_3, ['Year', 'Quarter'], "inner")

    # Drop duplicate columns from df_2 and df_3
    columns_to_drop = [col for col in merged_df.columns if col.endswith('_df2') or col.endswith('_df3')]
    merged_df = merged_df.drop(*columns_to_drop)

    # Clean column names
    for col_name in merged_df.columns:
        merged_df = merged_df.withColumnRenamed(col_name, clean_column_name(col_name))

    merged_df = merged_df.withColumn("Quarter", expr("replace(Quarter, 'Q', '')"))

    return merged_df




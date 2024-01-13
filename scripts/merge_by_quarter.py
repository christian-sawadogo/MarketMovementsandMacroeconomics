from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from Merge_Economic_Data import merge_func


def merge_dfs(spark, df2):

    # Google Cloud Storage paths
    bucket_name = "economic_data_oecd"
    path1 = f'gs://{bucket_name}/economic_data/QuarterlyGDPandComponents.csv'
    path2 = f'gs://{bucket_name}/economic_data/QuarterlyNationalAccounts.csv'
    path3 = f'gs://{bucket_name}/economic_data/QuarterlyRealGDPgrowth.csv'

    # Merge the DataFrames from different files
    df1 = merge_func(spark, path1, path2, path3)

    # Read data frame in and merge both data frames

    # Alias the 'Year' and 'Quarter' columns of the second DataFrame
    df2_aliased = df2.select(
        [col(c).alias(c + "_df2") if c in ['Year', 'Quarter'] else col(c) for c in df2.columns]
    )

    # Perform the join operation without ambiguity
    df_merged = df1.join(
        df2_aliased,
        (df1['Year'] == df2_aliased['Year_df2']) & (df1['Quarter'] == df2_aliased['Quarter_df2'])
    )

    columns_to_remove = ['Year_df2', 'Quarter_df2']
    df_merged = df_merged.drop(*columns_to_remove)

    return df_merged







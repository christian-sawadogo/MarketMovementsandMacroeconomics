from merge_all import process_corona
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import quarter


def cleanse_stocks(df, column, group):
    '''Filters DataFrame for particular group by relevant dates, adds columns by Year and Week.'''
    print(f'\nCleansing data for stocks in {group}...')

    # Only select necessary columns
    df = df.select('Date', column)

    # Convert Date string to PySpark date type
    df = df.withColumn("Date", F.to_date(F.col("Date"), "dd-MM-yyy"))

    # Filter data for relevant period (January 2006 to December 2022)
    df = df.filter((F.col("Date") >= "2005-01-01") & (F.col("Date") <= "2022-12-31"))

    # Add 'Group' column to DataFrame
    df = df.withColumn("Group", F.lit(group))

    # Calculate weekly average prices / weekly total volume
    df = df.withColumn("Year", F.year("Date"))
    df = df.withColumn("Quarter", quarter("Date"))
    if column == 'Volume':
        df = df.groupBy("Group", "Year", "Quarter").agg(F.sum(column).alias(column))
    else:
        df = df.groupBy("Group", "Year", "Quarter").agg(F.avg(column).alias(column))

    # Remove columns with null values in any row
    return df.na.drop()


def merge_by_group(stock_df, stock_column, stock_group, covid_df, write_path_final):
    '''Groups DataFrames for given stock market / sector by Year and Week calculating the average
    value for stock_column. Then merges with Covid data and returns the merged DataFrame.'''

    # Cleanse stock market data
    stock_df = cleanse_stocks(stock_df, stock_column, stock_group)

    # Merge with Covid data
    df = stock_df.join(covid_df, on=["Year", "Quarter"], how='leftouter')

    # Write to CSV file
    print(f'Writing to {write_path_final} ...')
    df.write.csv(write_path_final, header=True, mode="overwrite")

    return df


def calculate_extremes(df, column, group):
    '''Computes the weekly return by taking the ratio of the weekly day's price to the previous week's 
    price and subtracting 1. The highest performing stock is the one having the largest total return.
    Returns the lowest and the highest performing stock.'''
    # Step 1: Calculate weekly returns
    windowSpec = Window().partitionBy("Name").orderBy("Year", "Quarter")
    df_returns = df.withColumn("WeeklyReturn", F.col(column) / F.lag(column).over(windowSpec) - 1)

    # Step 2: Aggregate returns to get overall weekly performance
    df_performance = df_returns.groupBy("Name").agg(F.sum("QuarterlyReturn").alias("Performance"))

    # Step 3: Identify the worst and best performing stock
    worst = df_performance.orderBy("Performance").first()
    best = df_performance.orderBy(F.desc("Performance")).first()

    # Display the results
    print(f"The worst performing stock in {group} is:", worst["Name"])
    print(f"The best performing stock in {group} is:", best["Name"])

    return worst["Name"], best["Name"]


def cleanse_stocks(df, column, group):
    '''Filters DataFrame for particular group by relevant dates, adds columns by Year and Week.'''
    print(f'\nCleansing data for stocks in {group}...')

    # Only select relevant columns
    df = df.select('Date', 'Name', column)

    # Convert Date string to PySpark date type
    df = df.withColumn("Date", F.to_date(F.col("Date"), "dd-MM-yyy"))

    # Filter data for relevant period (January 2006 to December 2022)
    df = df.filter((F.col("Date") >= "2005-01-01") & (F.col("Date") <= "2022-12-31"))

    # Group by name, year and week
    df = df.withColumn("Year", F.year("Date"))
    df = df.withColumn("Quarter", quarter("Date"))
    if column == 'Volume':
        df = df.groupBy("Name", "Year", "Quarter").agg(F.sum(column).alias(column))
    else:
        df = df.groupBy("Name", "Year", "Quarter").agg(F.avg(column).alias(column))

    # Remove empty rows
    df = df.na.drop()

    # Filter out stocks with incomplete data (less than 200 rows)
    name_counts = df.groupBy("Name").count()
    df = df.join(name_counts, "Name").filter(F.col("count") >= 200)

    # Drop the count column
    return df.drop("count")


def find_for_market(spark, stock_column, stock_markets, covid_column, covid_area, read_path, write_path):
    '''Reads and finds highest/lowest performing stock in chosen markets. Then merges with Covid data.'''
    print("========================================================================================")

    result_df = None
    for market in stock_markets:
        # Read all stock files in market into one DataFrame
        print(f"\nReading stock files for {market}...")
        df = spark.read.csv(f"{read_path}/stock_market_data/{market}/", header=True, inferSchema=True)
        
        # Cleanse and group stocks by Name, Year and Week
        df = cleanse_stocks(df, stock_column, market)

        # Calculate extremes
        worst, best = calculate_extremes(df, stock_column, market)
        worst = str(worst)
        best = str(best)
        name_filter = [worst, best]

        # Filter by name, removing duplicates
        if result_df is None:
            result_df = df.filter(F.col("Name").isin(name_filter))
            continue
        
        if result_df.filter(F.col("Name") == worst).count() > 0:
            name_filter.remove(worst)
        if result_df.filter(F.col("Name") == best).count() > 0:
            name_filter.remove(best)
        
        result_df = result_df.unionAll(df.filter(F.col("Name").isin(name_filter)))

    # If all stock markets, find extremes
    if len(stock_markets) != 1: 
        worst, best = calculate_extremes(result_df, stock_column, 'all markets')
        result_df = result_df.filter(F.col("Name").isin(worst, best))

    # Merge with Covid data
    covid_df = process_corona(spark, covid_column, covid_area, read_path)
    result_df = result_df.join(covid_df, on=["Year", "Quarter"], how='leftouter')

    # Write to CSV file
    market = 'all' if len(stock_markets) != 1 else stock_markets[0]
    csv_path = f"{write_path}/CSVs/extremes/{market}_{stock_column}_{covid_area[1]}_{covid_column}.csv"
    print(f'Writing to {csv_path} ...')
    result_df.write.csv(csv_path, header=True, mode="overwrite")

    print("========================================================================================")

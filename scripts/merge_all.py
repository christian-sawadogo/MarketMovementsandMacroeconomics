from pyspark.sql import functions as F
from merge_by_group import merge_by_group
from pyspark.sql.functions import quarter
from merge_by_quarter import merge_dfs
from economic_data_plot import execute_and_save_all_plots


def process_corona(spark, column, area, read_path):
    '''Filters Covid data by chosen area and groups by Year and Week.'''
    print(f'\nFiltering and grouping Covid data...')

    # Read the CSV file into a DataFrame with header and schema inferred
    division = area[0]
    location = area[1]

    if division == 'oecd':
        division = 'country'

    df = spark.read.csv(f"{read_path}/covid_death_data/export_{division}.csv", header=True, inferSchema=True)

    # Filter by world / region / country
    df = df.filter(F.col(df.columns[0]) == location)

    # Select OECD Countries

    oecd_countries_iso3c = [
        "AUS", "AUT", "BEL", "CAN", "CHL", "COL",
        "CRI", "CZE", "DNK", "EST", "FIN",
        "FRA", "DEU", "GRC", "HUN", "ISL", "IRL",
        "ISR", "ITA", "JPN", "KOR", "LVA", "LTU",
        "LUX", "MEX", "NLD", "NZL", "NOR",
        "POL", "PRT", "SVK", "SVN", "ESP",
        "SWE", "CHE", "TUR", "GBR", "USA"
    ]

    if division == 'oecd':
        df = df.filter(df["iso3c"].isin(oecd_countries_iso3c))
        column_name = "iso3c"
        # Show the entire column (assuming the DataFrame is small enough)
        df.select(column_name).show(n=df.count(), truncate=False)
        df = df.drop(column_name)

    # Select only the necessary columns
    df = df.select(df.columns[0], 'date', column)

    # Convert the date string to a date format and extract the week and year
    df = df.withColumn("date", F.to_date(F.col("date"), 'yyyy-MM-dd'))
    df = df.withColumn("Quarter", quarter(F.col("date")))
    df = df.withColumn("Year", F.year(F.col("date")))

    # Group by 'year' and 'week_of_year' and then aggregate
    return df.groupBy(df.columns[0], "Year", "Quarter").agg(F.avg(F.col(column)).alias(column))


def merge_markets_covid(spark, stock_column, stock_markets, covid_column, covid_area, read_path, write_path):
    '''Reads and merges Covid and stock market data in chosen markets altogether.'''
    print("========================================================================================")

    # Define path to write CSVs
    csv_path = f"{write_path}/CSVs/general/all_{stock_column}_{covid_area[1]}_{covid_column}.csv"

    # Get Covid data
    covid_df = process_corona(spark, covid_column, covid_area, read_path)

    result_df = None
    for market in stock_markets:
        # Read all stock files in market into one DataFrame
        df = spark.read.csv(f"{read_path}/stock_market_data/{market}/", header=True, inferSchema=True)

        # Merge with Corona data
        df = merge_by_group(df, stock_column, market, covid_df, csv_path.replace('general/all_', f'general/{market}_'))

        # Merge with other stock markets
        result_df = result_df.unionAll(df) if result_df else df

    # If just one stock_market, necessary CSV are already generated
    if len(stock_markets) == 1:
        return

    print('\nMerging all Covid and stock market data...')

    # Group by Year, Week and covid_column
    if stock_column == 'Volume':
        result_df = result_df.groupBy('Year', 'Quarter', covid_column).agg(F.sum(stock_column).alias(stock_column))
    else:
        result_df = result_df.groupBy('Year', 'Quarter', covid_column).agg(F.avg(stock_column).alias(stock_column))

    # Write to CSV file
    print(f'Writing to {csv_path} ...')
    result_df.write.csv(csv_path, header=True, mode="overwrite")

    # Merge economic data

    eco_df = merge_dfs(spark, result_df)
    eco_path = f"gs://economic_analysis_results/{stock_column}_{covid_area[1]}_{covid_column}/data2.csv"
    eco_df.write.csv(eco_path, header=True, mode="overwrite")

    sd = "gs://economic_data_oecd_plots/"
    execute_and_save_all_plots(eco_df, sd)

    print("========================================================================================")


def merge_sectors_covid(spark, stock_column, sectors, covid_column, covid_area, read_path, write_path):
    '''Reads and merges Covid and stock data in individual sectors.'''
    print("========================================================================================")

    # Get Covid data
    covid_df = process_corona(spark, covid_column, covid_area, read_path)

    # Read categorized stocks file
    print("Reading and filtering stock data for each market...")
    sectors_df = spark.read.csv(f"{read_path}/categorized_stocks.csv", header=True, inferSchema=True)

    # Read stock files in each market
    stock_dfs = [spark.read.csv(f"{read_path}/stock_market_data/{market}/", header=True, inferSchema=True)
                 for market in ['sp500', 'forbes2000', 'nyse', 'nasdaq']]

    for sector in sectors:
        # Filter sectors DataFrame by sector
        filter_df = sectors_df.filter(sectors_df['Category'] == sector).select("Name")

        result_df = None
        for df in stock_dfs:
            # Filter stock DataFrame by sector
            df = df.join(filter_df, on=['Name'])

            # Merge with other stock markets
            result_df = result_df.unionAll(df) if result_df else df

            # Remove used stock names to avoid duplicates
            filter_df = filter_df.subtract(df.select("Name"))

        # Merge with Corona data
        csv_path = f"{write_path}/CSVs/general/{sector}_{stock_column}_{covid_area[1]}_{covid_column}.csv"
        eco_path = f"gs://economic_analysis_results/{sector}_{stock_column}_{covid_area[1]}_{covid_column}/data.csv"
        result_df = merge_by_group(result_df, stock_column, sector, covid_df, csv_path)
        eco_df = merge_dfs(spark, result_df)
        eco_df.write.csv(eco_path, header=True, mode="overwrite")

        sd = "gs://economic_data_oecd_plots"
        execute_and_save_all_plots(eco_df, sd)

    print("========================================================================================")

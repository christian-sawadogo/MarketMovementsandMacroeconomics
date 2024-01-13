import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from matplotlib.ticker import MaxNLocator
from google.cloud import storage


def modify_spark_df(df_spark):
    # Select relevant columns
    df_spark_selected = df_spark.select('Year', 'Quarter', 'Gross_domestic_product_Total_economy_1',
                                        'Exports_of_goods_and_services_4',
                                        'Imports_of_goods_and_services_5', 'Gross_capital_formation__'
                                                                           'Gross_fixed_capital_formation_Total_economy_4',
                                        'daily_covid_deaths', 'Close')

    # Sort the DataFrame by Year and then by Quarter
    df_spark_sorted = df_spark_selected.orderBy('Year', 'Quarter')

    return df_spark_sorted


def plot_gdp_stock(spark_df, gcs_path):
    # Convert the PySpark DataFrame to a Pandas DataFrame
    data = spark_df.toPandas()

    # Parsing 'Year' and 'Quarter' into a single datetime column for plotting
    data['Date'] = pd.to_datetime(data['Year'].astype(str) + 'Q' + data['Quarter'].astype(str))

    # Sorting data to ensure it's in the correct order
    data.sort_values('Date', inplace=True)

    # Converting GDP and Close values to floats
    data['Gross_domestic_product_Total_economy_1'] = data['Gross_domestic_product_Total_economy_1'].str.replace('.',
                                                                                                                '').str.replace(
        ',', '.').astype(float)
    data['Close'] = data['Close'].astype(float)

    # Create a new figure and axis for the GDP plot
    fig, ax1 = plt.subplots(figsize=(14, 7))

    # Plot the GDP data
    color = 'tab:green'
    ax1.set_xlabel('Time (Year and Quarter)')
    ax1.set_ylabel('Gross Domestic Product (GDP)', color=color)
    ax1.plot(data['Date'], data['Gross_domestic_product_Total_economy_1'], color=color)
    ax1.tick_params(axis='y', labelcolor=color)
    ax1.xaxis.set_major_locator(MaxNLocator(integer=True))

    # Create a second axis for the stock performance plot
    ax2 = ax1.twinx()
    color = 'tab:blue'
    ax2.set_ylabel('Stock Performance (Close)', color=color)
    ax2.plot(data['Date'], data['Close'], color=color)
    ax2.tick_params(axis='y', labelcolor=color)

    # Title the graph
    plt.title('GDP and Stock Performance Over Time')

    # Show a legend and the plot
    fig.tight_layout()
    plt.legend()

    # Save the plot to a temporary local file
    temp_local_path = '/tmp/plot.png'
    plt.savefig(temp_local_path, bbox_inches='tight')
    plt.close()

    # Upload the local file to GCS
    client = storage.Client()
    bucket_name, blob_name = gcs_path.replace('gs://', '').split('/', 1)
    blob = client.get_bucket(bucket_name).blob(blob_name)
    blob.upload_from_filename(temp_local_path)


def convert_to_float(column):
    # Function to handle different numeric formats in the DataFrame
    if column.dtype == object:
        return column.str.replace('.', '', regex=False).str.replace(',', '.', regex=False).astype(float)
    return column


def plot_additional_time_series(df_spark_modified, gcs_path):
    data = df_spark_modified.toPandas()

    # Convert 'Year' and 'Quarter' into a datetime string
    data['Date'] = pd.to_datetime(data['Year'].astype(str) + data['Quarter'].astype(str).str.zfill(2), format='%Y%m')

    # Convert numerical columns
    data['Gross_domestic_product_Total_economy_1'] = convert_to_float(data['Gross_domestic_product_Total_economy_1'])
    data['Exports_of_goods_and_services_4'] = convert_to_float(data['Exports_of_goods_and_services_4'])
    data['Imports_of_goods_and_services_5'] = convert_to_float(data['Imports_of_goods_and_services_5'])
    data['Gross_capital_formation__Gross_fixed_capital_formation_Total_economy_4'] = convert_to_float(
        data['Gross_capital_formation__Gross_fixed_capital_formation_Total_economy_4'])
    data['daily_covid_deaths'] = convert_to_float(data['daily_covid_deaths'])
    data['Close'] = convert_to_float(data['Close'])

    # Create a new figure with multiple subplots
    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(14, 15), sharex=True)

    # Plot GDP and Stock Performance in the first subplot
    ax1.plot(data['Date'], data['Gross_domestic_product_Total_economy_1'], color='green', label='GDP')
    ax1.set_ylabel('GDP', color='green')
    ax1.tick_params(axis='y', labelcolor='green')
    ax1.legend(loc='upper left')

    ax12 = ax1.twinx()
    ax12.plot(data['Date'], data['Close'], color='blue', linestyle='--', label='Stock Performance')
    ax12.set_ylabel('Stock Performance (Close)', color='blue')
    ax12.tick_params(axis='y', labelcolor='blue')
    ax12.legend(loc='upper right')

    # Plot Exports and Imports in the second subplot
    ax2.plot(data['Date'], data['Exports_of_goods_and_services_4'], color='orange', label='Exports')
    ax2.plot(data['Date'], data['Imports_of_goods_and_services_5'], color='blue', label='Imports')
    ax2.set_ylabel('Exports & Imports')
    ax2.legend(loc='upper left')

    # Plot Gross Capital Formation in the third subplot
    ax3.plot(data['Date'], data['Gross_capital_formation__Gross_fixed_capital_formation_Total_economy_4'],
             color='purple', label='Gross Capital Formation')
    ax3.set_ylabel('Gross Capital Formation', color='purple')
    ax3.tick_params(axis='y', labelcolor='purple')
    ax3.legend(loc='upper left')

    # Overlay Covid-19 impact on the first subplot as a bar graph
    ax1.bar(data['Date'], data['daily_covid_deaths'], width=20, label='Daily Covid Deaths', color='grey', alpha=0.3)
    ax1.legend(loc='lower left')

    # Annotations for the 2008 financial crisis and Covid-19 pandemic
    crisis_2008_start = pd.to_datetime('2007-07-01')
    crisis_2008_end = pd.to_datetime('2009-03-31')
    covid_start = pd.to_datetime('2020-01-01')

    for ax in [ax1, ax2, ax3]:
        ax.axvspan(crisis_2008_start, crisis_2008_end, color='grey', alpha=0.3, label='2008 Financial Crisis')
        ax.axvline(x=covid_start, color='red', linestyle='--', label='Covid-19 Pandemic Start')

    # Set the x-axis major locator to integer for better readability
    ax3.xaxis.set_major_locator(MaxNLocator(integer=True))
    ax3.set_xlabel('Time (Year and Quarter)')

    # Title the graph
    plt.suptitle('GDP, Stock Performance, and Additional Economic Indicators Over Time')

    # Adjust layout
    fig.tight_layout(rect=[0, 0.03, 1, 0.95])

    # Save the plot to a temporary local file
    temp_local_path = '/tmp/plot.png'
    plt.savefig(temp_local_path, bbox_inches='tight')
    plt.close()

    # Upload the local file to GCS
    client = storage.Client()
    bucket_name, blob_name = gcs_path.replace('gs://', '').split('/', 1)
    blob = client.get_bucket(bucket_name).blob(blob_name)
    blob.upload_from_filename(temp_local_path)


def plot_combination_chart(df_spark, gcs_path):
    # Modify and convert the PySpark DataFrame to a Pandas DataFrame
    df_spark_modified = modify_spark_df(df_spark)
    data = df_spark_modified.toPandas()

    # Convert 'Year' and 'Quarter' into a datetime string
    data['Date'] = pd.to_datetime(data['Year'].astype(str) + data['Quarter'].astype(str).str.zfill(2), format='%Y%m')

    # Convert numerical columns
    data['Gross_domestic_product_Total_economy_1'] = convert_to_float(data['Gross_domestic_product_Total_economy_1'])
    data['Exports_of_goods_and_services_4'] = convert_to_float(data['Exports_of_goods_and_services_4'])
    data['Imports_of_goods_and_services_5'] = convert_to_float(data['Imports_of_goods_and_services_5'])
    data['Gross_capital_formation__Gross_fixed_capital_formation_Total_economy_4'] = convert_to_float(
        data['Gross_capital_formation__Gross_fixed_capital_formation_Total_economy_4'])
    data['Close'] = convert_to_float(data['Close'])

    # Create a new figure
    fig, ax1 = plt.subplots(figsize=(14, 7))

    # Line chart for continuous data (GDP and Stock Performance)
    ax1.plot(data['Date'], data['Gross_domestic_product_Total_economy_1'], color='green', label='GDP')
    ax1.set_ylabel('GDP', color='green')
    ax1.tick_params(axis='y', labelcolor='green')

    ax2 = ax1.twinx()
    ax2.plot(data['Date'], data['Close'], color='blue', linestyle='--', label='Stock Performance')
    ax2.set_ylabel('Stock Performance (Close)', color='blue')
    ax2.tick_params(axis='y', labelcolor='blue')

    # Bar chart for discrete data (Exports, Imports, and Investment)
    width = 20  # Width of the bars
    ax1.bar(data['Date'], data['Exports_of_goods_and_services_4'], width, label='Exports', alpha=0.3, color='orange')
    ax1.bar(data['Date'], data['Imports_of_goods_and_services_5'], width, label='Imports', alpha=0.3, color='red',
            bottom=data['Exports_of_goods_and_services_4'])
    ax1.bar(data['Date'], data['Gross_capital_formation__Gross_fixed_capital_formation_Total_economy_4'], width,
            label='Investment', alpha=0.3, color='purple')

    # Legend and titles
    ax1.legend(loc='upper left')
    ax2.legend(loc='upper right')
    plt.title('GDP, Stock Performance, and Quarterly Changes in Economic Indicators')

    # Save the plot to a temporary local file
    temp_local_path = '/tmp/plot.png'
    plt.savefig(temp_local_path, bbox_inches='tight')
    plt.close()

    # Upload the local file to GCS
    client = storage.Client()
    bucket_name, blob_name = gcs_path.replace('gs://', '').split('/', 1)
    blob = client.get_bucket(bucket_name).blob(blob_name)
    blob.upload_from_filename(temp_local_path)


def plot_default_correlation_analysis(df_spark_modified, gcs_path):
    data = df_spark_modified.toPandas()

    # Convert GDP and Stock Performance columns to float
    data['Gross_domestic_product_Total_economy_1'] = convert_to_float(data['Gross_domestic_product_Total_economy_1'])
    data['Close'] = convert_to_float(data['Close'])

    # Create scatter plot with regression line
    plt.figure(figsize=(10, 6))
    sns.regplot(x='Gross_domestic_product_Total_economy_1', y='Close', data=data)
    plt.title('Correlation between GDP and Stock Market Performance')
    plt.xlabel('Gross Domestic Product')
    plt.ylabel('Stock Market Performance')

    # Save the plot to a temporary local file
    temp_local_path = '/tmp/plot.png'
    plt.savefig(temp_local_path, bbox_inches='tight')
    plt.close()

    # Upload the local file to GCS
    client = storage.Client()
    bucket_name, blob_name = gcs_path.replace('gs://', '').split('/', 1)
    blob = client.get_bucket(bucket_name).blob(blob_name)
    blob.upload_from_filename(temp_local_path)


def execute_and_save_all_plots(df_spark, save_directory):
    # Ensure the directory exists
    import os
    if not os.path.exists(save_directory):
        os.makedirs(save_directory)

    # Modify Spark DataFrame
    df_spark_modified = modify_spark_df(df_spark)

    # Execute each plot function and save the plots
    plot_gdp_stock(df_spark_modified, os.path.join(save_directory, 'gdp_stock.png'))
    plot_additional_time_series(df_spark_modified, os.path.join(save_directory, 'additional_time_series.png'))
    plot_combination_chart(df_spark_modified, os.path.join(save_directory, 'combination_chart.png'))
    plot_default_correlation_analysis(df_spark_modified,
                                      os.path.join(save_directory, 'default_correlation_analysis.png'))

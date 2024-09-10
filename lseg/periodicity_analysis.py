'''
Purpose: This script is designed to analyze the country count data for different banks and regions. 
It loads the country count data from an Excel file, cleans it, and then analyzes the data for 
different regions and countries. It calculates summary statistics for the distances between reports 
for each region and country. It then plots histograms and line plots for the non-empty data.

Author: Oliver Wang
Date: July 10, 2024
Version: 1.0
Details:
- Load and clean data for a specific bank.
- Analyze data for specified regions and countries.
- Plot histograms for non-empty data.
- Plot line plots for non-empty data.
- Save summary statistics for each bank to separate CSV files.
'''

#==================================================================================================
# Import necessary libraries
#==================================================================================================
import pandas as pd
import matplotlib.pyplot as plt
import math
import os

#==================================================================================================
# Main function to organize the workflow
#==================================================================================================
def main():
    """
    Main function to organize the workflow.
    """
    banks = ['jpmorgan', 'morgan_stanley', 'ubs_equities', 'deutsche_bank', 'bofa_global_research', 'nomura']
    regions = ['Global', 'Europe', 'Latin America', 'GEM', 'ASEAN']
    countries = ['US', 'India', 'Australia', 'China', 'Brazil', 'Japan', 'UK', 'Mexico', 'Russia', 'Africa', 'Singapore']
    
    for bank in banks:
        # Load and clean data
        cleaned_csv_path = load_and_clean_data('datastore/derived/reports/lseg/country_count/country_count_final.xlsx', bank)
        
        # Analyze data
        summary_stats, non_empty_hist_axes, non_empty_line_axes = analyze_data(cleaned_csv_path, regions, countries)
        
        # Save summary statistics for the bank
        summary_stats_df = pd.DataFrame(summary_stats)
        summary_stats_df.to_csv(f'datastore/derived/reports/lseg/country_count/{bank}_summary_stats.csv')
        
        # Load cleaned data for plotting
        data = pd.read_csv(cleaned_csv_path)
        
        # Plot histograms
        plot_histograms(data, non_empty_hist_axes, regions, countries, bank)
        
        # Plot line plots
        plot_line_plots(data, non_empty_line_axes, regions, countries, bank)
    
    print("All processing and plotting are complete, and saved to the specified folder.")

#==================================================================================================
# Function to load and clean data for a specific bank
#==================================================================================================
def load_and_clean_data(file_path, bank):
    """
    Load and clean data for a specific bank.

    Parameters:
    file_path (str): Path to the Excel file.
    bank (str): Name of the bank.

    Returns:
    str: Path to the cleaned CSV file.
    """
    # Load Excel file
    df = pd.read_excel(file_path)
    df_bank = df[df['Bank'] == bank]
    
    # Save as CSV
    csv_path = f'country_count/{bank}_country_count.csv'
    df_bank.to_csv(csv_path, index=False)
    
    # Load CSV file
    data = pd.read_csv(csv_path)
    
    # Add a helper column for the first 28 characters of Report Identifier
    data['Report Identifier Prefix'] = data['Report Identifier'].str[:28]
    
    # Find duplicate rows based on the specified conditions
    duplicates = data[
        (data['Days Since 1990'] == data['Days Since 1990'].shift(1)) &
        (data['country'] == data['country'].shift(1)) &
        (data['region'] == data['region'].shift(1)) &
        (data['Report Identifier Prefix'] == data['Report Identifier Prefix'].shift(1))
    ]
    
    # Drop the found duplicate rows
    cleaned_data = data.drop(duplicates.index)
    
    # Drop fully duplicate rows
    cleaned_data_final = cleaned_data.drop_duplicates(subset=['Days Since 1990', 'country', 'region', 'Report Identifier Prefix'])
    
    # Drop rows with empty 'region' and 'country'
    cleaned_data_final = cleaned_data_final.dropna(subset=['region', 'country'])
    
    # Save the final cleaned data
    cleaned_csv_path = f'datastore/derived/reports/lseg/country_count/cleaned_{bank}_country_count.csv'
    cleaned_data_final.to_csv(cleaned_csv_path, index=False)
    
    return cleaned_csv_path

#===============================================================================================
# Function to analyze data for regions and countries
#===============================================================================================
def analyze_data(file_path, regions, countries):
    """
    Analyze data for specified regions and countries.

    Parameters:
    file_path (str): Path to the cleaned CSV file.
    regions (list): List of regions to analyze.
    countries (list): List of countries to analyze.

    Returns:
    tuple: Summary statistics, non-empty histogram axes, and non-empty line plot axes.
    """
    # Load cleaned CSV file
    data = pd.read_csv(file_path)
    
    # Convert 'Date' column to datetime
    data['Date'] = pd.to_datetime(data['Date'])
    
    summary_stats = {}
    non_empty_hist_axes = []
    non_empty_line_axes = []
    
    # Combine regions and countries into one list
    regions_and_countries = regions + countries
    
    # Loop through each region and country to calculate summary statistics
    for name in regions_and_countries:
        if name in regions:
            filtered_reports = data[data['region'].str.contains(name, na=False)].copy()
        else:
            filtered_reports = data[data['country'].str.contains(name, na=False)].copy()
        
        if not filtered_reports.empty:
            filtered_reports = filtered_reports.sort_values(by='Date')
            filtered_reports['Distance (days)'] = filtered_reports['Date'].diff().dt.days
            filtered_reports = filtered_reports[filtered_reports['Distance (days)'] < 30]
            
            if not filtered_reports.empty:
                stats = filtered_reports['Distance (days)'].describe()
                summary_stats[name] = stats
                non_empty_hist_axes.append(name)
                non_empty_line_axes.append(name)
    
    return summary_stats, non_empty_hist_axes, non_empty_line_axes

#===============================================================================================
# Function to plot histograms for non-empty data
#===============================================================================================
def plot_histograms(data, non_empty_hist_axes, regions, countries, bank):
    """
    Plot histograms for non-empty data.

    Parameters:
    data (DataFrame): The data to plot.
    non_empty_hist_axes (list): List of non-empty histogram axes.
    regions (list): List of regions to analyze.
    countries (list): List of countries to analyze.
    bank (str): Name of the bank.
    """
    num_hist_plots = len(non_empty_hist_axes)
    num_cols_hist = 4
    num_hist_rows = math.ceil(num_hist_plots / num_cols_hist)
    
    fig_hist, axes_hist = plt.subplots(nrows=num_hist_rows, ncols=num_cols_hist, figsize=(20, num_hist_rows * 5))
    axes_hist = axes_hist.flatten()
    
    for idx, name in enumerate(non_empty_hist_axes):
        if name in regions:
            filtered_reports = data[data['region'].str.contains(name, na=False)].copy()
        else:
            filtered_reports = data[data['country'].str.contains(name, na=False)].copy()
        
        filtered_reports = filtered_reports.sort_values(by='Date')
        filtered_reports['Distance (days)'] = filtered_reports['Date'].diff().dt.days
        filtered_reports = filtered_reports[filtered_reports['Distance (days)'] < 30]
        
        ax = axes_hist[idx]
        filtered_reports['Distance (days)'].plot(kind='hist', bins=20, ax=ax, edgecolor='black')
        ax.set_title(f'Histogram of Distances for {name} ({bank})')
        ax.set_xlabel('Distance (days)')
        ax.set_ylabel('Frequency')
    
    for j in range(idx + 1, len(axes_hist)):
        fig_hist.delaxes(axes_hist[j])
    
    plt.tight_layout()
    plt.savefig(f'datastore/derived/reports/lseg/country_count/{bank}_histograms.png')
    plt.close(fig_hist)

#===============================================================================================
# Function to plot line plots for non-empty data
#===============================================================================================
def plot_line_plots(data, non_empty_line_axes, regions, countries, bank):
    """
    Plot line plots for non-empty data.

    Parameters:
    data (DataFrame): The data to plot.
    non_empty_line_axes (list): List of non-empty line plot axes.
    regions (list): List of regions to analyze.
    countries (list): List of countries to analyze.
    bank (str): Name of the bank.
    """
    num_line_plots = len(non_empty_line_axes)
    num_cols_line = 4
    num_line_rows = math.ceil(num_line_plots / num_cols_line)
    
    fig_line, axes_line = plt.subplots(nrows=num_line_rows, ncols=num_cols_line, figsize=(20, num_line_rows * 5))
    axes_line = axes_line.flatten()
    
    for idx, name in enumerate(non_empty_line_axes):
        if name in regions:
            filtered_reports = data[data['region'].str.contains(name, na=False)].copy()
        else:
            filtered_reports = data[data['country'].str.contains(name, na=False)].copy()
        
        filtered_reports = filtered_reports.sort_values(by='Date')
        filtered_reports['Distance (days)'] = filtered_reports['Date'].diff().dt.days
        filtered_reports = filtered_reports[filtered_reports['Distance (days)'] < 30]
        
        ax = axes_line[idx]
        distance_counts = filtered_reports['Distance (days)'].value_counts().sort_index()
        distance_counts.plot(kind='line', ax=ax, marker='o')
        ax.set_title(f'Frequency of Distances for {name} ({bank})')
        ax.set_xlabel('Distance (days)')
        ax.set_ylabel('Frequency')
    
    for j in range(idx + 1, len(axes_line)):
        fig_line.delaxes(axes_line[j])
    
    plt.tight_layout()
    plt.savefig(f'datastore/derived/reports/lseg/country_count/{bank}_line_plots.png')
    plt.close(fig_line)

#===============================================================================================
# Run the main function
#===============================================================================================
if __name__ == "__main__":
    main()

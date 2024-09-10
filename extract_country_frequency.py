'''
Purpose: To extract the most frequent country mentioned in the text files within zip files
         in a directory and save the counts to a CSV file.
Author: Oliver Wang
Date: May 22, 2024
Details: This script processes text files within zip archives to identify and count mentions of countries.
         It uses the pycountry library to recognize country names and saves the results to a CSV file.
         The script includes the following main parts:
         (1) Load country data from pycountry.
         (2) Iterate over all zip files in the directory.
         (3) Check each text file within the zip files.
         (4) Find the most frequent country in each text file.
         (5) Update the country counts.
         (6) Create a DataFrame from the counts and save it to a CSV file.
'''

# =====================================================================
# Import required modules
# =====================================================================
import os
import zipfile
import pycountry
import re
from collections import Counter
import pandas as pd

# =====================================================================
# Define global variables
# =====================================================================
countries = [country.name for country in pycountry.countries]  # Load country data

# Directory Paths
INPUT_DIR = 'datastore/derived/reports/lseg/bank_zip'
OUTPUT_CSV_PATH = 'datastore/derived/reports/lseg/word_cloud/country_counts.csv'

# =====================================================================
# Main execution function
# =====================================================================
def main():
    """Execute the process to find the most frequent country in txt files within zip archives."""
    country_counts = Counter()

    # Iterate over zip files in the directory
    for file in os.listdir(INPUT_DIR):
        if file.endswith('.zip'):
            zip_path = os.path.join(INPUT_DIR, file)
            loop_through_zipfiles(zip_path, country_counts)

    # Convert the counts to a DataFrame and save as CSV
    df = pd.DataFrame(list(country_counts.items()), columns=['Country', 'Count']).set_index('Country')
    df.to_csv(OUTPUT_CSV_PATH)
    
# =====================================================================
# Define functions to loop through zip files and text files and find the most frequent country
# =====================================================================
def loop_through_zipfiles(zip_path, country_counts):
    """
    Process each text file within a zip file to update country counts.
    
    Args:
        zip_path (str): Path to the zip file.
        country_counts (Counter): Counter object to store country counts.
    """
    with zipfile.ZipFile(zip_path, 'r') as z:
        for filename in z.namelist():
            if filename.endswith('.txt'):
                # Call function to loop through text files and find the most frequent country
                loop_through_text_files(z, filename, country_counts)

def loop_through_text_files(zip_file, filename, country_counts):
    """
    Read and process a single text file from a zip to find and count the most frequent country.
    
    Args:
        zip_file (ZipFile): ZipFile object for the zip archive.
        filename (str): Name of the text file within the zip archive.
        country_counts (Counter): Counter object to store country counts.
    """
    with zip_file.open(filename) as f:
        # Read the text file and find the most frequent country
        text = f.read().decode('utf-8')
        # Call function to find the most frequent country
        most_common_country = find_most_frequent_country(text)
        # Update the country counts
        if most_common_country:
            country_counts[most_common_country] += 1

def find_most_frequent_country(text):
    """Extract the most frequent country from text using regex matching."""
    # Use regex to find country names in the text
    pattern = r'\b(' + '|'.join(re.escape(country) for country in countries) + r')\b'
    # Find all occurrences of country names in the text
    found_countries = re.findall(pattern, text, re.IGNORECASE)
    # Count the occurrences of each country and find the most common
    country_counts = Counter(country.capitalize() for country in found_countries)
    most_common_country = country_counts.most_common(1)[0][0] if country_counts else None
    return most_common_country

# =====================================================================
# Check if script is main
# =====================================================================
if __name__ == "__main__":
    main()

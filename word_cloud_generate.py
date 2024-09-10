'''
Purpose: To generate circular word clouds from CSV files containing keyword counts, and save the word cloud as PDF files 
            in the same directory according to the country information.
Author: Oliver Wang
Date: May 27, 2024
'''
# =====================================================================
# Import required modules
# =====================================================================
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from wordcloud import WordCloud
# =====================================================================
# Load the CSV file and define the output PDF file path for the word cloud
# =====================================================================
country_frequency_csv_file = 'datastore/derived/reports/lseg/word_cloud/country_risk_frequencies.csv'
word_cloud_path = 'datastore/derived/reports/lseg/word_cloud/country_risk_wordclouds.pdf'
df = pd.read_csv(country_frequency_csv_file)

# =====================================================================
# Define the numbers of the plots per page and the grid size
# =====================================================================
num_countries = df.shape[0]
plots_per_page = 9  # Number of plots per page
# =====================================================================
# Define Main Function
# =====================================================================
def main():
    # Create a PDF file to save the word clouds
    with PdfPages(word_cloud_path) as pdf:
        # Loop through the countries and create a word cloud for each country
        for start in range(0, num_countries, plots_per_page):
            # Get the countries for the current page
            end = min(start + plots_per_page, num_countries)
            # Get the data for the current page of countries from start to end
            page_countries = df.iloc[start:end]
            # Create a figure with subplots for each country
            grid_size = (3, 3)  # 3x3 grid per page
            fig, axes = plt.subplots(nrows=grid_size[0], ncols=grid_size[1], figsize=(20, 20))
            axes = axes.flatten()
            # Loop through the countries on the current page and create a word cloud for each country
            for i, (index, row) in enumerate(page_countries.iterrows()):
                # Get the country name and data from the csv file
                country_name = row['Country']
                country_data = row.drop('Country')
            
                wordcloud = create_word_cloud_single_keywords(country_data, country_name)
            
                ax = axes[i]
                ax.imshow(wordcloud, interpolation='bilinear')
                ax.set_title(country_name, fontsize=24)
                ax.axis('off')
        
            # Hide any unused subplots
            for j in range(i + 1, len(axes)):
                axes[j].axis('off')
        
            # Save the figure to the PDF
            pdf.savefig(fig)
            plt.close(fig)

# =====================================================================
# Define function to create word cloud for a single country
# =====================================================================
def create_word_cloud_single_keywords(country_data, country_name):
    '''
    This function creates a word cloud for a single country based on the 
    keyword frequencies in the country_data dictionary.
    Args:
        country_data (dict): A dictionary containing keyword frequencies for a country.
        country_name (str): The name of the country.
    '''
    # Create a text string with the keywords repeated based on their frequency
    text_string = ' '.join([category for category, frequency in country_data.items() if int(frequency) > 0])
    # Check if the text string is empty then assign 'no_data' to it
    if text_string.strip() == '':
        text_string = 'no_data'
    wordcloud = WordCloud(width=400, height=400, background_color='white').generate(text_string)
    return wordcloud

# =====================================================================
# Call the main function
# =====================================================================
if __name__ == '__main__':
    main()


  
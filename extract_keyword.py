'''
Purpose: To find the most frequent country and keywords in text files within zip archives, and save the results to a CSV file.
Author: Oliver Wang
Date: May 27, 2024
Details:
- The script reads text files within zip archives in a specified directory.
- It extracts the most frequent country mentioned in each text file.
- It performs LDA analysis to extract topics and find the frequency of predefined risk keywords in each topic.
- The script saves the risk frequencies for each country to a CSV file.
- The script uses spaCy for text processing, Gensim for LDA analysis, and scikit-learn for clustering.
- The predefined list of common words and risk keywords are used to filter the text.
- The results are saved to 'datastore/derived/reports/lseg/country_risk_frequencies.csv'.
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
import spacy
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
import nltk
from gensim import corpora
from gensim.models import LdaModel
from extract_country_frequency import find_most_frequent_country
# =====================================================================
# Download necessary NLTK resources
# =====================================================================
nltk.download('stopwords')
nltk.download('punkt')

# =====================================================================
# Load spaCy English model and define global variables
# =====================================================================
try:
    nlp = spacy.load('en_core_web_sm')
except OSError:
    # Download the model if not found and load it again
    from spacy.cli import download
    download('en_core_web_sm')
    nlp = spacy.load('en_core_web_sm')

# Increase the maximum length limit for spaCy to handle long texts
nlp.max_length = 200000000  # Adjust this value as needed

# Load country data
countries = [country.name for country in pycountry.countries]

# Predefined list of common words to exclude
common_words = set([
    'equity', 'decline', 'market', 'stock', 'year', 'sector', 'college', 'issue', 'fall', 'lead', 'report',
    'analyst', 'information', 'research', 'company', 'investment', 'material', 'expect', 'estimate', 'price',
    'new', 'week', 'month', 'security', 'capital', 'source', 'return', 'financial', 'group', 'result',
    'document', 'distribute', 'service', 'june', 'january', 'february', 'march', 'april', 'may', 'june',
    'july', 'august', 'september', 'october', 'november', 'december', 'monday', 'tuesday', 'wednesday',
    'thursday', 'friday', 'saturday', 'sunday', 'buy', 'europe', 'head', 'fellow', 'fall', 'winter',
    'summer', 'rise', 'spring', 'strategy', 'harvard', 'boston', 'growth', 'impact', 'table', 'reg', 'day',
    'daily', 'monthly', 'annual', 'head', 'score', 'datastream', 'uk', 'us', 'european', 'financial',
    'economic', 'growth', 'trend', 'publication', 'london', 'negative', 'surprise', 'sector', 'branch',
    'index', 'jan', 'new', 'forecast', 'valuation', 'correlation', 'fax', 'peak', 'top', 'fund', 'brazil',
    'change', 'flow', 'bofa', 'stanley', 'morgan', 'chart', 'expect', 'plan', 'current', 'coverage',
    'securities', 'figure', 'net', 'case', 'itau', 'point', 'weight', 'trading', 'sale', 'performance',
    'dividend', 'exhibit', 'consumer', 'return', 'material', 'number', 'flow', 'tax', 'high', 'low',
    'business', 'economist', 'fellow', 'yes', 'no', 'total', 'bond', 'factor', 'change'
])

# Define risk keywords
risk_keywords = {
    'political': ['political', 'government', 'policy', 'election'],
    'currency': ['currency', 'exchange', 'forex', 'fx'],
    'market': ['market', 'stock', 'equity', 'share'],
    'default risk': ['default', 'insolvency', 'bankrupt', 'failure'],
    'rating risk': ['rating', 'rating agency', 'credit score', 'creditworthiness'],
    'debt risk': ['debt', 'leverage', 'borrowing', 'loan'],
    'liquidity': ['liquidity', 'cash', 'solvency'],
    'operational': ['operational', 'operation', 'process', 'management'],
    'reputational': ['reputational', 'reputation', 'brand'],
    'legal': ['legal', 'law', 'regulation', 'compliance'],
    'strategic': ['strategic', 'strategy', 'plan', 'planning'],
    'interest rate': ['interest rate', 'rate', 'yield'],
    'commodity': ['commodity', 'oil', 'gold', 'commodity'],
    'concentration': ['concentration', 'focus', 'core'],
    'volatility': ['volatility', 'volatility', 'variation'],
    'systemic': ['systemic', 'system', 'infrastructure'],
    'compliance': ['compliance', 'regulation', 'regulatory'],
    'environmental': ['environmental', 'environment', 'climate', 'sustainability'],
    'technological': ['technological', 'technology', 'tech'],
    'cyber': ['cyber', 'cybersecurity', 'hacking', 'data'],
    'inflation': ['inflation', 'price', 'cost'],
    'geopolitical': ['geopolitical', 'geo', 'political'],
    'economic': ['economic', 'economy', 'gdp', 'growth']
}

# LDA Hyperparameters
NUM_TOPICS = 5
PASSES = 15

# Directory Paths
INPUT_DIR = 'datastore/derived/reports/lseg/bank_zip'
OUTPUT_CSV_PATH = 'datastore/derived/reports/lseg/word_cloud/country_risk_frequencies.csv'

# =====================================================================
# Main execution function
# =====================================================================
def main():
    """Execute the process to find the most frequent country and keywords in txt files within zip archives."""
    country_risk_frequencies = {}

    # Iterate over zip files in the directory
    for file in os.listdir(INPUT_DIR):
        if file.endswith('.zip'):
            zip_path = os.path.join(INPUT_DIR, file)
            loop_through_zipfiles(zip_path, country_risk_frequencies)

    # Save the risk frequencies for each country to a CSV file
    save_country_risk_frequencies_to_csv(country_risk_frequencies, OUTPUT_CSV_PATH)

    print(f'Results saved to {OUTPUT_CSV_PATH}')

# =====================================================================
# Define functions to loop through zip files and text files and find the most frequent country and keywords
# =====================================================================
def loop_through_zipfiles(zip_path, country_risk_frequencies):
    """
    Process each text file within a zip file to update country risk frequencies.

    Args:
        zip_path (str): Path to the zip file.
        country_risk_frequencies (dict): Dictionary to store risk frequencies for each country.
    """
    with zipfile.ZipFile(zip_path, 'r') as z:
        for filename in z.namelist():
            if filename.endswith('.txt'):
                # Call function to loop through text files and find the most frequent country and keywords
                loop_through_text_files(z, filename, country_risk_frequencies)

def loop_through_text_files(zip_file, filename, country_risk_frequencies):
    """
    Read and process a single text file from a zip to find and count the most frequent country and extract keywords.

    Args:
        zip_file (ZipFile): ZipFile object for the zip archive.
        filename (str): Name of the text file within the zip archive.
        country_risk_frequencies (dict): Dictionary to store risk frequencies for each country.
    """
    with zip_file.open(filename) as f:
        # Read the text file and find the most frequent country
        text = f.read().decode('utf-8')
        most_common_country = find_most_frequent_country(text)

        # Update the country risk frequencies
        if most_common_country:
            if most_common_country not in country_risk_frequencies:
                country_risk_frequencies[most_common_country] = {key: [0] * NUM_TOPICS for key in risk_keywords}
            
            lda_topics = perform_lda_analysis(text)
            for topic_id, topic in lda_topics:
                topic_keywords = topic.split(' + ')
                for risk_type, keywords in risk_keywords.items():
                    risk_count = sum(any(keyword in word for keyword in keywords) for word in topic_keywords)
                    country_risk_frequencies[most_common_country][risk_type][topic_id] += risk_count

# =====================================================================
# Define functions to preprocess text and perform LDA analysis
# =====================================================================
def perform_lda_analysis(text):
    """Perform LDA analysis on the preprocessed text."""
    tokens = tokenize_text(text)

    # Create dictionary and corpus
    dictionary = corpora.Dictionary([tokens])
    corpus = [dictionary.doc2bow(tokens)]

    # Train LDA model
    lda_model = LdaModel(corpus, num_topics=NUM_TOPICS, id2word=dictionary, passes=PASSES)

    # Extract topics
    topics = lda_model.print_topics(num_words=10)
    topic_list = [(i, topic[1]) for i, topic in enumerate(topics)]

    return topic_list

def tokenize_text(text):
    """
    Preprocess the text by converting to lowercase, removing stop words, lemmatizing, and excluding common words.

    Args:
        text (str): The input text to preprocess.

    Returns:
        list: The preprocessed tokens.
    """
    doc = nlp(text.lower())
    tokens = [token.lemma_ for token in doc if not token.is_stop and token.is_alpha and token.lemma_ not in common_words]
    return tokens

# =====================================================================
# Define function to save country risk frequencies to a CSV file
# =====================================================================
def save_country_risk_frequencies_to_csv(country_risk_frequencies, output_csv_path):
    """Save the risk frequencies for each country to a CSV file."""
    risk_df_list = []
    for country, risks in country_risk_frequencies.items():
        total_risks = {risk: sum(counts) for risk, counts in risks.items()}
        total_risks['Country'] = country
        risk_df_list.append(total_risks)

    # Convert the list of dictionaries to a DataFrame
    risk_df = pd.DataFrame(risk_df_list)

    # Save the DataFrame to CSV
    risk_df.to_csv(output_csv_path, index=False)

# =====================================================================
# Check if script is main
# =====================================================================
if __name__ == "__main__":
    main()

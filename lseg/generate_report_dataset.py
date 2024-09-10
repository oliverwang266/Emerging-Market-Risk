'''
Purpose: To generate the dataset of reports of LSEG with report identifiers,
dates, banks, countries, and risk categories and form into a csv file.
Author: Oliver Wang
Date: June 6, 2024
Details: The script processes all text reports within zip files in a given directory to extract information such as dates, banks, countries, and risk categories.
    - The date is extracted from the filename.
    - The bank name is inferred from the filename.
    - The country is determined by the most frequent country mentioned in the text.
    - The risk categories are identified using LDA analysis and predefined risk keywords.
    - The script uses spaCy for text processing, NLTK for stopwords, and Gensim for LDA analysis.
    - The final dataset is saved as a CSV file with the extracted information.
'''
# =====================================================================
# Import required modules
# =====================================================================
import os
import re
import zipfile
from collections import Counter
import pandas as pd
import spacy
import nltk
from gensim import corpora
from gensim.models import LdaModel
import pycountry
from extract_country_frequency import find_most_frequent_country
from extract_keyword import tokenize_text
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
    from spacy.cli import download
# Download the English model if not found
    download('en_core_web_sm')
    nlp = spacy.load('en_core_web_sm')

# =====================================================================
# Increase the maximum length limit for spaCy to handle long texts
# =====================================================================
nlp.max_length = 200000000

# =====================================================================
# Load country data
# =====================================================================
countries = [country.name for country in pycountry.countries]

# =====================================================================
# Predefined list of common words to exclude
# =====================================================================
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

# =====================================================================
# Define risk keywords
# =====================================================================
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

# =====================================================================
# Directory Paths
# =====================================================================
INPUT_DIR = 'datastore/derived/reports/lseg/bank_zip'
OUTPUT_CSV_PATH = 'datastore/derived/reports/lseg/report_data.csv'

# =====================================================================
# LDA Hyperparameters
# =====================================================================
NUM_TOPICS = 5
PASSES = 15
NUM_WORDS = 10

# =====================================================================
# Main execution function
# =====================================================================
def main():
    report_data = []

    for file in os.listdir(INPUT_DIR):
        if file.endswith('.zip'):
            extract_from_zipfile(file, report_data)

    df = pd.DataFrame(report_data)
    df.to_csv(OUTPUT_CSV_PATH, index=False)
    print(df)
# =====================================================================
# Define functions to enter zip files and extract information from text files
# =====================================================================
def extract_from_zipfile(file, report_data):
    '''
    Extract information from text files within a zip archive.
    Args:
    file (str): Name of the zip file.
    report_data (list): List to store extracted information.
    '''
    zip_path = os.path.join(INPUT_DIR, file)
    with zipfile.ZipFile(zip_path, 'r') as z:
        for filename in z.namelist():
            if not filename.endswith('/'): # Skip directories
                extract_from_text(z, filename, report_data)

def extract_from_text(zip_file, filename, report_data):
    '''
    Extract information from a single text file within a zip archive.
    Args:
    zip_file (ZipFile): ZipFile object for the zip archive.
    filename (str): Name of the text file within the zip archive.
    report_data (list): List to store extracted information.
    '''
    if filename.endswith('.txt'):
        with zip_file.open(filename) as f:
        # Read the text file and decode it as UTF-8
            text = f.read().decode('utf-8')
            '''
            date is extracted from the filename;
            bank name is from the zip folder name;
            country is the most frequent country in the text;
            risk categories are extracted using LDA analysis.
            report_id is the filename without the extension.
            '''
            date = extract_date(filename)
            bank = os.path.basename(zip_file.filename).split('.')[0] # Get the bank name from the zip folder name
            country = find_most_frequent_country(text)
            risk_dummies = extract_risk_categories(text)
            report_id = filename[5:].replace('.txt', '')
            # Append the extracted information to the report_data list
            report_data.append({
                'Report Identifier': report_id,
                'Date': date,
                'Bank': bank,
                'Country': country,
                **risk_dummies
            })

# =====================================================================
# Define functions to extract date from the filename
# =====================================================================
def extract_date(filename):
    '''
    Use regex to extract the date in the format 'YYYY-MM-DD',
    for example, '2005-11-01' from the filename: '2005-11-01-abg_sundal_collier....txt'
    '''
    match = re.search(r'(\d{4}-\d{2}-\d{2})', filename)
    if match:
        return match.group(1)
    return None

# =====================================================================
# Define functions to preprocess text and perform LDA analysis
# =====================================================================
def perform_lda_analysis(text):
    # Preprocess the text and create dictionary and corpus
    tokens = tokenize_text(text)
    if len(tokens) == 0:
        return []
    # create dictionary and corpus for LDA analysis using Gensim
    dictionary = corpora.Dictionary([tokens])
    corpus = [dictionary.doc2bow(tokens)]
    if len(corpus[0]) == 0:
        return []
    lda_model = LdaModel(corpus, num_topics=NUM_TOPICS, id2word=dictionary, passes=PASSES)
    topics = lda_model.print_topics(num_words=NUM_WORDS)
    return [(i, topic[1]) for i, topic in enumerate(topics)]

# =====================================================================
# Define function to extract risk categories using LDA analysis
# =====================================================================
def extract_risk_categories(text):
    # Extract risk categories using LDA analysis and predefined risk keywords, generate risk dummies
    lda_topics = perform_lda_analysis(text)
    # Initialize risk dummies to 0
    risk_dummies = {risk: 0 for risk in risk_keywords}
    for topic_id, topic in lda_topics:
    # Split the topic into keywords and count the occurrences of risk keywords
        topic_keywords = topic.split(' + ')
    # Check if any risk keyword is present in the topic
        for risk_type, keywords in risk_keywords.items():
    # Count the occurrences of risk keywords in the topic
            risk_count = sum(any(keyword in word for keyword in keywords) for word in topic_keywords)
            if risk_count > 0:
                risk_dummies[risk_type] = 1
    return risk_dummies

# =====================================================================
# Main function call
# =====================================================================
if __name__ == "__main__":
    main()

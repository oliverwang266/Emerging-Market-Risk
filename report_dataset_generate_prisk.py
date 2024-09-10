import os
import re
import zipfile
from collections import Counter
import pandas as pd
import spacy
import nltk

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
# Increase spaCy's maximum length limit to handle long texts
# =====================================================================
nlp.max_length = 200000000

# =====================================================================
# Load country data
# =====================================================================
import pycountry
countries = [country.name for country in pycountry.countries]

# =====================================================================
# Predefined list of common words to exclude
# =====================================================================
common_words = set(nltk.corpus.stopwords.words('english'))

# =====================================================================
# Directory path
# =====================================================================
INPUT_DIR = 'datastore/derived/reports/lseg/bank_zip'
OUTPUT_CSV_PATH = 'datastore/derived/reports/lseg/report_data_prisk.csv'

# =====================================================================
# Main execution function
# =====================================================================
def main():
    # Initialize report_data list to store extracted information
    report_data = []
    # Load bigrams and synonyms from text files
    political_bigrams = load_bigrams('datastore/derived/reports/lseg/training_library/political_textbook.txt')
    # Load non-political bigrams from text file
    non_political_bigrams = load_bigrams('datastore/derived/reports/lseg/training_library/non_political_textbook.txt')
    # Load synonyms from text file
    synonyms = load_synonyms('datastore/derived/reports/lseg/training_library/synonyms.txt')

    print(f"Loaded {len(political_bigrams)} political bigrams")
    print(f"Loaded {len(non_political_bigrams)} non-political bigrams")
    print(f"Loaded {len(synonyms)} synonyms")
    # Iterate through all files in the input directory
    for file in os.listdir(INPUT_DIR):
        if file.endswith('.zip'):
            extract_from_zipfile(file, report_data, political_bigrams, non_political_bigrams, synonyms)
    # Convert report_data list to a DataFrame and save to CSV file
    df = pd.DataFrame(report_data)
    df.to_csv(OUTPUT_CSV_PATH, index=False)
    print(df)

# =====================================================================
# Define function to enter zip file and extract information from text files
# =====================================================================
def extract_from_zipfile(file, report_data, political_bigrams, non_political_bigrams, synonyms):
    '''
    Extract information from text files within a zip file.
    Parameters:
    file (str): zip file name.
    report_data (list): List to store extracted information.
    ''' 
    zip_path = os.path.join(INPUT_DIR, file)
    with zipfile.ZipFile(zip_path, 'r') as z:
        for filename in z.namelist():
            if not filename.endswith('/'):  # Skip directories
                extract_from_text(z, filename, report_data, political_bigrams, non_political_bigrams, synonyms)

def extract_from_text(zip_file, filename, report_data, political_bigrams, non_political_bigrams, synonyms):
    '''
    Extract information from a single text file within a zip file.
    Parameters:
    zip_file (ZipFile): ZipFile object of the zip file.
    filename (str): Text file name within the zip file.
    report_data (list): List to store extracted information.
    '''
    if filename.endswith('.txt'):
        with zip_file.open(filename) as f:
            # Read text file and decode to UTF-8
            text = f.read().decode('utf-8')
            '''
            Date extracted from the filename;
            Bank name inferred from the zip folder name;
            Country is the most mentioned country in the text;
            Risk categories analyzed using PRisk and NPRisk.
            Report ID is the filename without the extension.
            '''
            date = extract_date(filename)
            bank = os.path.basename(zip_file.filename).split('.')[0]  # Get bank name from zip folder name
            country = find_most_frequent_country(text)
            prisk = calculate_risk(text, political_bigrams, non_political_bigrams, synonyms)
            nprisk = calculate_risk(text, non_political_bigrams, political_bigrams, synonyms)
            report_id = filename.split('/')[-1].replace('.txt', '')
            # Append extracted information to the report_data list
            report_data.append({
                'Report Identifier': report_id,
                'Date': date,
                'Bank': bank,
                'Country': country,
                'PRisk': prisk,
                'NPRisk': nprisk
            })

            print(f"Processed report: {report_id}, Date: {date}, Bank: {bank}, Country: {country}, PRisk: {prisk}, NPRisk: {nprisk}")

# =====================================================================
# Define function to extract date from filename
# =====================================================================
def extract_date(filename):
    '''
    Extract date from filename using regex in 'YYYY-MM-DD' format,
    e.g., extract '2005-11-01' from filename '2005-11-01-abg_sundal_collier....txt'
    '''
    match = re.search(r'(\d{4}-\d{2}-\d{2})', filename)
    if match:
        return match.group(1)
    return None

# =====================================================================
# Define function to find the most mentioned country in the text
# =====================================================================
def find_most_frequent_country(text):
    '''
    Find the most mentioned country in the text.
    Parameters:
    text (str): Input text.
    '''
    # Use regex to find country names in the text and count occurrences
    pattern = r'\b(' + '|'.join(re.escape(country) for country in countries) + r')\b'
    found_countries = re.findall(pattern, text, re.IGNORECASE)
    country_counts = Counter(country.capitalize() for country in found_countries)
    # Find the most common country
    most_common_country = country_counts.most_common(1)[0][0] if country_counts else None
    return most_common_country

# =====================================================================
# Define function to load bigram frequencies
# =====================================================================
def load_bigrams(filename):
    stop_words = set(nltk.corpus.stopwords.words('english'))
    with open(filename, 'r', encoding='utf-8') as file:
        text = file.read().lower()
        text = re.sub(r'\d+', '', text)  # Remove all numbers
        words = re.findall(r'\b\w+\b', text)
        words = [word for word in words if word not in stop_words]  # Remove common words
        bigrams = [' '.join(pair) for pair in zip(words, words[1:])]
        bigram_counts = Counter(bigrams)
    return bigram_counts

# =====================================================================
# Define function to load synonyms of risks
# =====================================================================
def load_synonyms(filename):
    with open(filename, 'r', encoding='utf-8') as file:
        synonyms = file.read().lower().split()
        return set(synonyms)

# =====================================================================
# Define function to extract bigrams from text
# =====================================================================
def extract_bigrams(text):
    text = re.sub(r'\d+', '', text)  # Remove all numbers
    words = re.findall(r'\b\w+\b', text.lower())
    words = [word for word in words if word not in common_words]  # Remove common words
    bigrams = [' '.join(pair) for pair in zip(words, words[1:])]
    return bigrams, words
# =====================================================================
# Define function to calculate risk score
# =====================================================================
def calculate_risk(text, bigrams_target, bigrams_other, synonyms):
    bigrams, words = extract_bigrams(text)
    bigram_counts = Counter(bigrams)
    
    risk_count = 0
    total_bigrams = len(bigrams)
    
    for i, word in enumerate(words):
        if word in synonyms:
            context_bigrams = bigrams[max(0, i-10):min(len(bigrams), i+10)]
            for bigram in context_bigrams:
                if bigram in bigrams_target and bigram not in bigrams_other:
                    risk_count += bigram_counts[bigram]
    
    risk = risk_count / total_bigrams if total_bigrams > 0 else 0
    return risk

# =====================================================================
# Execute main function
# =====================================================================
if __name__ == "__main__":
    main()

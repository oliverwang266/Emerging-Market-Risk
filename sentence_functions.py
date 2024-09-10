'''
Purpose: Functions for sentence processing.
Author: Xiangyu Chen
Date: July 29, 2024
Details:

- Functions to get and filter sentences:
    - Sentence tokenization from paragraph
    - Sentence filtering
    
- Functions for template extraction:
    - Counting similar sentences
    - Deciding templates according to the count

- Functions for operating on all reports:
    - Get templates
    - Remove templates
    
'''

from global_settings import *
import pickle
import json
from tqdm import tqdm
import nltk
import random
from collections import Counter
from rapidfuzz import process, fuzz

def get_sents(paragraph_text):
    return nltk.sent_tokenize(paragraph_text)

def sent_filter(text, stop_words):
    # Too short
    if len(text) < 10:
        return False
    # Too few words
    if len(text.split()) < 5:
        return False
    # Include stop words
    for s in stop_words:
        if s in text:
            return False
        
    # Too many digits
    if any(c.isdigit() for c in text):
        # If too many digits
        digit_chars = sum(c.isdigit() for c in text)
        if digit_chars / len(text) > 0.1:
            return False
        
    return True


def get_sent_count(flat_sents, score_cutoff=80):
    '''
    Count the frequency of each sentence and return a dictionary of similar sentences.
    
    Args:
        flat_sents (list): List of sentences.
        score_cutoff (int): Minimum score for sentences to be considered similar.
    '''
    # Count of each sentence
    sent_count = Counter(flat_sents)
    # Get unique sentences
    flat_sents = set(flat_sents)
    
    templates = {}
    checked = set()

    for s in tqdm(flat_sents):
        # Skip if already checked
        if s in checked:
            continue
        else:
            checked.add(s)
            
        # Count of current sentence
        temp_count = sent_count[s]
        # Get all sentences that are not checked
        sents_to_check = flat_sents.difference(checked)

        # Get similar sentences
        temp = process.extract(s, sents_to_check, scorer=SIMILARITY_ALGO, score_cutoff=score_cutoff, limit=None)
        # Update checked set and count
        for t in temp:
            checked.add(t[0])
            temp_count += sent_count[t[0]] # Add count of similar sentence
            
        # Add to templates
        templates[s] = {
            'n': temp_count,
            'matches': [t[0] for t in temp]
        }

    # Return sorted templates
    return dict(sorted(templates.items(), key=lambda x: x[1]['n'], reverse=True))

def sent_count_to_template(sent_count, lower_bound=3):
    '''
    Transform the sentence count dictionary to a list of sentences that appear more than the lower bound.
    
    Args:
        sent_count (dict): Dictionary of sentence count.
        lower_bound (int): Minimum number of appearance to be included in the template.
    '''
    return [key for key, value in sent_count.items() if value['n'] >= lower_bound]

def get_templates(report_sents):    
    # Get templates
    template_dict = {}
    for bank_name, items in report_sents.items():
        bank_sents = []
        for item in items:
            id = item['id']
            sents = item['res']
            bank_sents.append(sents)

        bank_sents = random.sample(bank_sents, min(NUM_TO_CHECK, len(bank_sents)))
        flat_sents = [sent for sublist in bank_sents for sent in sublist]

        sent_count = get_sent_count(flat_sents, score_cutoff=SIMILARITY_THRESHOLD)
        template = sent_count_to_template(sent_count, lower_bound=max(len(bank_sents) * TEMPLATE_PROB, TEMPLATE_APPEARANCE))
        template_dict[bank_name] = template

    return template_dict

def remove_templates(report_sents, template_dict):
    # Remove templates
    clean_sents = {}
    for bank_name, items in report_sents.items():
        clean_bank_sents = []
        for item in tqdm(items):
            id = item['id']
            sents = item['res']
            # Bank template
            bank_template = template_dict[bank_name]

            valuable_sents = []
            useless_sents = []

            # Iterate over sentences
            for s in sents:
                # Check if sent is in template
                is_useless = False

                for t in bank_template:
                    if fuzz.QRatio(s, t) >= SIMILARITY_THRESHOLD:
                        is_useless = True
                        break

                if is_useless:
                    useless_sents.append(s)
                else:
                    valuable_sents.append(s)

            clean_bank_sents.append({
                'id': id,
                'sents': valuable_sents,
                'sents_removed': useless_sents
            })
        clean_sents[bank_name] = clean_bank_sents
        # print(clean_sents)
    return clean_sents
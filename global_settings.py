'''
Purpose: Global settings for the whole project.
Author: Xiangyu Chen
Date: July 29, 2024
Details:
- Paths to the data files.
- Global variables for:
    - Template extraction
'''

from pathlib import Path
import zipfile
import os
from tqdm import tqdm
from rapidfuzz import fuzz

DATA_PATH = Path('datastore')
LSEG_DATA = DATA_PATH / 'derived' / 'reports' / 'lseg'

# =====================================================================
# Global variables
# =====================================================================

# Template Extraction
NUM_TO_CHECK = 20
TEMPLATE_PROB = 0.2
TEMPLATE_APPEARANCE = 5
SIMILARITY_THRESHOLD = 80
SIMILARITY_ALGO = fuzz.QRatio

'''
Purpose: to convert bank equity analyst reports in PDF format to .txt files for text analysis.
Author: Oliver Wang
Date: May 7, 2024
Details: The script has the following main parts:
    (1) Takes PDFs from the bank-level zipfiles and converts them to text files; 
    (2) Appends the text files into a bank-level zipfile.
'''

# %% ==================================================================
# Load required modules
# =====================================================================

import os
import zipfile
import pdfplumber

# %% ==================================================================
# Define main function
# =====================================================================

def main() -> None:
    """
    Main function to initiate the converting text from original PDF files in the input directory
    and store the text files in the output directory.

    Defines the paths for input and output data and directly includes the processing code.
    """
    # Define the input and output directories
    input_pdf_directory = 'datastore/raw/reports/lseg/orig'
    output_txt_directory = 'datastore/derived/reports/lseg/bank_zip'
    
    # Ensure the output directory exists
    os.makedirs(output_txt_directory, exist_ok=True)
    
    # Process each zipfile in the input directory
    for filename in os.listdir(input_pdf_directory):
        # Check if the file is a zip file
        if filename.endswith('.zip'):
            # Define the input and output paths
            input_zip_path = os.path.join(input_pdf_directory, filename)
            output_zip_path = os.path.join(output_txt_directory, filename)
            # Open the input ZIP for reading and the output ZIP for appending compressed files
            with zipfile.ZipFile(input_zip_path, 'r') as input_zip, \
             zipfile.ZipFile(output_zip_path, 'a', zipfile.ZIP_DEFLATED) as output_zip:
                # Extract text from PDF files and write to text files calling the function
                extract_text_from_pdf(input_zip, output_zip)
# =====================================================================
# Define a function to extract text from a PDF file and write it to a text file
# =====================================================================
def extract_text_from_pdf(input_zip: zipfile.ZipFile, output_zip: zipfile.ZipFile) -> None:
    """
    Extract text from PDF files within the input zipfile and write them as text files in the output zipfile.

    Args:
        input_zip (zipfile.ZipFile): The input zipfile containing PDF files.
        output_zip (zipfile.ZipFile): The output zipfile to write text files.
    """
    # Get the existing files in the output zip to avoid overwriting
    existing_files = set(output_zip.namelist())
    # Iterate over each file in the input zip
    for analyst_report in input_zip.namelist():
        # Check if the file is a PDF and a corresponding text file does not exist
        if analyst_report.endswith('.pdf'):
            txt_file_path = analyst_report[:-4] + '.txt'
            if txt_file_path not in existing_files:
                try:
                    # Extract text from the PDF file and write it to a text file in the output zip
                    with input_zip.open(analyst_report) as file, pdfplumber.open(file) as pdf:
                        full_text = [page.extract_text() for page in pdf.pages if page.extract_text()]
                        full_text = '\n'.join(full_text)
                        output_zip.writestr(txt_file_path, full_text)
                # Handle exceptions and log errors for specific files due to corruption or other issues
                except Exception as e:
                    print(f"Error processing {analyst_report}: {e}")
# =====================================================================
# Define a function to clean filenames by replacing disallowed characters with underscores
# =====================================================================

def clean_filename(filename: str) -> str:
    """
    Sanitize the filename by replacing any characters not allowed in filenames
    with underscores, ensuring compatibility with the file system.

    Args:
        filename (str): The original filename to be cleaned.

    Returns:
        str: The cleaned filename.
    """
    allowed_chars = set('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 -_.')
    return ''.join(c if c in allowed_chars else '_' for c in filename)

# %% ==================================================================
# Execute the main function
# =====================================================================

if __name__ == "__main__":
    main()

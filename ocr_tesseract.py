# bsub -J "myArray[1-5]" -M 24G -n 6 /usr/local/app/rcs_bin/grid3/envs/rcs_2023.09/bin/python /export/projects4/mmiller_emrisk/ocr/ocr_tesseract_jobarray.py
import subprocess
import math
import time
import os
import random
import sqlite3

from io import StringIO
import pandas as pd
import pickle

import toolkit as tk

#==================================================================================================
# Functions
#==================================================================================================

def run_ocr(images, layout_table, image_ppi, layout_ppi, lang, buffer_size=2, temp_path='sample.tiff'):
    # Transform bbox to list if it is a string
    # Rename page_idx to page_index to merge with OCR results
    layout_table = tk.preprocess_layout_df(layout_table)

    # Crop the images and get the DataFrame of page_idx, block_idx, and image
    image_df = crop_images(images, layout_table, image_ppi, layout_ppi, buffer=buffer_size)
    images = image_df['image'].to_list() # Get the images
    image_df.drop(columns='image', inplace=True) # Drop the images column to save memory

    # Do OCR for the images
    ocr_res = tesseract_ocr_multi('tesseract', images, lang, temp_path=temp_path)

    # Add the OCR results to the DataFrame
    try:
        image_df['text'] = ocr_res
    except:
        print('Error in adding OCR results to DataFrame')
        print('Dataframe:')
        print(image_df)
        print('OCR Results:')
        print(ocr_res)
        raise Exception('Error in adding OCR results to DataFrame')

    return image_df

def crop_images(images, layout_table, image_ppi, layout_ppi, buffer=2):
    # Get the layout table
    scale = image_ppi / layout_ppi
    layout_table['bbox'] = layout_table['bbox'].apply(lambda x: tk.scale_bbox(x, scale, buffer=buffer))

    # Crop the images
    image_dict = []
    for page_index, img in enumerate(images):
        page_layout_df = layout_table[layout_table['page_index'] == page_index]
        bbox_num = len(page_layout_df)
        # Will not process pages with more than 255 bounding boxes
        if bbox_num >= 255:
            continue    
        for index, row in page_layout_df.iterrows():
            bbox_index = row['block_idx']
            x, y, w, h = row['bbox']
            cropped_img = img.crop((x, y, w, h))
            temp = {
                'page_idx': page_index,
                'block_idx': bbox_index,
                'image': cropped_img
            }
            image_dict.append(temp)
    # To DataFrame
    image_df = pd.DataFrame(image_dict)
    return image_df


def tesseract_ocr_multi(tesseract_cmd, imgs, lang, temp_path='sample.tiff'):
    # Save the images as a multi-page tiff to be read by tesseract
    imgs[0].save(str(temp_path), save_all=True, append_images=imgs[1:])

    time.sleep(0.1) # Wait for the file to be saved and avoid conflicts
    params = [
        tesseract_cmd, # Path to tesseract
        temp_path, # Input file
        'stdout', # Output to stdout
        '-l', lang, # English language
        '--oem', '1', # OCR Engine Mode 1, Use LSTM
        '--psm', '3', # Page segmentation mode 3
    ]
    
    try:
        result = subprocess.run(
            params,
            capture_output=True,
            text=True,
            encoding='utf-8'
        )
    except: # Rerun and print the error
        result = subprocess.run(
            params,
            capture_output=True,
            text=True,
            encoding='utf-8'
        )
        # Print output and errors
        print(f"Error:\n{result.stderr}\n")
        print(f"Output:\n{result.stdout}\n")

    output_text = result.stdout
    
    if output_text == None:
        print('Output text is None')
        return ["" for _ in range(len(imgs))]
    
    splitted = output_text.split('\f')

    time.sleep(0.1) # Wait for the file to be saved and avoid conflicts
    os.remove(temp_path) # Remove the temporary file

    return splitted

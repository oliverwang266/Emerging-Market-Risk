import fitz
from PIL import Image
import numpy as np
import math


def preprocess_layout_df(layout_df):
    layout_df.rename(columns={'page_idx': 'page_index'}, inplace=True)
    if type(layout_df['bbox'][0]) == str:
        layout_df['bbox'] = layout_df['bbox'].apply(lambda x: eval(x))

    return layout_df

def preprocess_img(img):
    # To grayscale
    img = img.convert('L')
    return img


def scale_bbox(bbox, scale, buffer=2):
    x, y, w, h = bbox
    x = math.floor((x - buffer) * scale)
    y = math.floor((y - buffer) * scale)
    w = math.ceil((w + buffer) * scale) # Add 2 to the width to avoid cutting the text
    h = math.ceil((h + buffer) * scale) # Add 2 to the height to avoid cutting the text
    
    return x, y, w, h

def add_unique_id(surya_df):
    '''
    Support function to add unique_id to the dataframe
    '''
    surya_df['bbox'] = surya_df['bbox'].apply(lambda x: eval(x))
    surya_df['bbox'] = surya_df['bbox'].apply(lambda x: [int(i) for i in x])
    surya_df['unique_id'] = surya_df['page_idx'].astype(str) + '_' + surya_df['bbox'].apply(lambda x: '_'.join([str(i) for i in x]))
    return surya_df

def pdfbytes2imgs(pdf_bytes, ppi=72):
    '''
    Transform the PDF file into images.
    PyMuPDF is faster than pdf2image, but pdf2image is more stable.

    Args:
    pdf_bytes (bytes): The PDF file in bytes.

    Returns:
    imgs (list): A list of images. Each image is a numpy array.
    '''
    
    pdf = fitz.open('pdf', pdf_bytes)
    imgs = pdf2images(pdf, ppi)
    return imgs
    
def pdf2images(pdf: fitz.Document, ppi=72):
    '''
    Transform the PDF file into images.
    PyMuPDF is faster than pdf2image, but pdf2image is more stable.

    Args:
    pdf (fitz.Document): The PDF object.

    Returns:
    imgs (list): A list of images. Each image is a Image object.
    '''

    imgs = []
    scale = ppi / 72
    for pg in range(0, pdf.page_count):
        page = pdf[pg]
        mat = fitz.Matrix(scale, scale)
        pm = page.get_pixmap(matrix=mat, alpha=False)

        img = Image.frombytes("RGB", [pm.width, pm.height], pm.samples)
        # img = cv2.cvtColor(np.array(img), cv2.COLOR_RGB2BGR)
        imgs.append(img)
    return imgs

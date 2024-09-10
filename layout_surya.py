'''
Purpose: This script transforms unstructured PDF reports into structured data using Surya.

PS: This script should be run on HBS's server, as we have too many reports to process.

Author: Xiangyu Chen
Date: July 29, 2024
Version: 1.0
Details:
- Connect to the LSEG database.
- Load the Surya models.
- Run layout analysis.
- Save the results to the database.
'''

#==================================================================================================
# Libraries
#==================================================================================================

# Third-party libraries
import pandas as pd

# Toolkit
import toolkit as tk

# Surya-OCR
from surya.detection import batch_text_detection
from surya.layout import batch_layout_detection
from surya.model.detection.model import load_model as load_detection_model, load_processor as load_detection_processor
from surya.settings import settings

from surya.ordering import batch_ordering
from surya.model.ordering.processor import load_processor as load_ordering_processor
from surya.model.ordering.model import load_model as load_ordering_model


# 32 for 16GB GPU
# RTX 4090 Laptop and V100 are all 16GB
settings.DETECTOR_BATCH_SIZE = 32

#==================================================================================================
# Functions
#==================================================================================================

def run_surya(images, det_model, det_processor, layout_model, layout_processor, ordering_model, ordering_processor):
    # Run layout analysis
    layout_preds = do_layout(
        images,
        det_model=det_model,
        det_processor=det_processor,
        layout_model=layout_model,
        layout_processor=layout_processor,
    )
    layout_df = parse_layout_res(layout_preds)

    # Get bboxes for reading order and analysis
    bboxes = get_bboxes(layout_preds)
    # Count the number of boxes
    page_box_num = [len(b) for b in bboxes]

    # Do reading order prediction
    if max(page_box_num) < 255: # Surya has a limit of 255 boxes per batch
        order_function = do_order
    else: # If reach the limit
        order_function = do_order_by_page #do ordering by pages and fill the rest with the former page's result

    order_predictions = order_function(
        images,
        bboxes,
        ordering_model=ordering_model,
        ordering_processor=ordering_processor,
    )

    order_df = parse_order_res(order_predictions)

    surya_df = order_layout(layout_df, order_df)
    return surya_df

def order_layout(layout_df, order_df):
    # Order the layout_df based on order_df
    # Switch bbox to int list and add unique_id
    layout_df = tk.add_unique_id(layout_df)
    order_df = tk.add_unique_id(order_df)

    # Add position (order index) to layout_df
    position_map = dict(zip(order_df['unique_id'], order_df['position']))
    layout_df['position'] = layout_df['unique_id'].map(position_map)

    # For some pages, there are too much bbox, and there is no ordering result for them
    # So after mapping, they will be nan
    # We need to replace them with -1 to keep consistency for int type
    layout_df['position'] = layout_df['position'].fillna(-1)
    layout_df['position'] = layout_df['position'].astype(int)
    layout_df.drop(columns=['unique_id'], inplace=True)
    # Sort by page_idx and position
    layout_df.sort_values(by=['page_idx', 'position'], inplace=True)
    return layout_df

def do_layout(images, det_model, det_processor, layout_model, layout_processor):
    line_predictions = batch_text_detection(images, det_model, det_processor)
    layout_predictions = batch_layout_detection(images, layout_model, layout_processor, line_predictions)
    return layout_predictions

def do_order(images, bboxes, ordering_model, ordering_processor):
    order_predictions = batch_ordering(images, bboxes, ordering_model, ordering_processor)
    return order_predictions

def do_order_by_page(images, bboxes, ordering_model, ordering_processor):
    '''
    Do ordering by pages.
    Notes: Surya has a limit of 255 boxes per batch.
    If 
    '''
    # Cut images and bboxes into batches
    batch_size = 1
    image_batches = [images[i:i + batch_size] for i in range(0, len(images), batch_size)]
    bboxes_batches = [bboxes[i:i + batch_size] for i in range(0, len(bboxes), batch_size)]
    
    # Do ordering in batches
    order_predictions = []
    for image_batch, bbox_batch in zip(image_batches, bboxes_batches):
        if get_box_num(bbox_batch) > 255:
            # Use the last batch_len boxes as the prediction
            temp_pred = order_predictions[-batch_size:]
            print("Too many boxes in this batch. Use the result of former page as the prediction.")
        else:
            temp_pred = do_order(image_batch, bbox_batch, ordering_model, ordering_processor)
        order_predictions.extend(temp_pred)
    return order_predictions   


def parse_layout_res(layout_predictions):
    '''
    Transform the layout predictions into a DataFrame.
    '''
    res_list = []
    for page_idx, page in enumerate(layout_predictions):
        image_bbox = page.image_bbox
        for block_idx, block in enumerate(page.bboxes):
            polygon = block.polygon
            confidence = block.confidence
            label = block.label
            bbox = block.bbox
            res_list.append({
                'page_idx': page_idx,
                'image_bbox': image_bbox,
                'block_idx': block_idx,
                'polygon': polygon,
                'confidence': confidence,
                'label': label,
                'bbox': bbox
            })
            
    res_df = pd.DataFrame(res_list)
    
    res_df['page_idx'] = res_df['page_idx'].astype(int)
    res_df['image_bbox'] = res_df['image_bbox'].astype(str)
    res_df['block_idx'] = res_df['block_idx'].astype(int)
    res_df['polygon'] = res_df['polygon'].astype(str)
    res_df['confidence'] = res_df['confidence'].astype(float)
    res_df['label'] = res_df['label'].astype(str)
    res_df['bbox'] = res_df['bbox'].astype(str)

    return res_df


def parse_order_res(order_predictions):
    '''
    Transform the ordering predictions into a DataFrame.
    '''
    res_list = []
    for page_idx, page in enumerate(order_predictions):
        image_bbox = page.image_bbox
        for block_idx, block in enumerate(page.bboxes):
            bbox = block.bbox
            position = block.position
            res_list.append({
                'page_idx': page_idx,
                'image_bbox': image_bbox,
                'block_idx': block_idx,
                'bbox': bbox,
                'position': position,
            })
    res_df = pd.DataFrame(res_list)
    
    res_df['page_idx'] = res_df['page_idx'].astype(int)
    res_df['image_bbox'] = res_df['image_bbox'].astype(str)
    res_df['block_idx'] = res_df['block_idx'].astype(int)
    res_df['bbox'] = res_df['bbox'].astype(str)
    res_df['position'] = res_df['position'].astype(int)
    
    return res_df
        
def get_bboxes(layout_predictions):
    '''
    Get the bounding boxes from the layout predictions.
    '''
    bboxes = []
    
    for page_idx, page in enumerate(layout_predictions):
        temp = []
        for block_idx, block in enumerate(page.bboxes):
            temp.append(block.bbox)
        bboxes.append(temp)
        
    return bboxes

def load_surya_models():
    layout_model = load_detection_model(checkpoint=settings.LAYOUT_MODEL_CHECKPOINT)
    layout_processor = load_detection_processor(checkpoint=settings.LAYOUT_MODEL_CHECKPOINT)
    det_model = load_detection_model()
    det_processor = load_detection_processor()
    ordering_model = load_ordering_model()
    ordering_processor = load_ordering_processor()
    
    return {
        'layout_model': layout_model,
        'layout_processor': layout_processor,
        'det_model': det_model,
        'det_processor': det_processor,
        'ordering_model': ordering_model,
        'ordering_processor': ordering_processor
    }
    
def get_box_num(bboxes):
    return sum([len(b) for b in bboxes])


from toolkit import pdfbytes2imgs, scale_bbox
from PIL import Image, ImageDraw, ImageFont
import sys
import math
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle


# If windows system
if sys.platform == 'win32':
    global_font_path = 'arial.ttf'
elif sys.platform == 'darwin':
    global_font_path = '/Library/Fonts/Arial.ttf'
elif sys.platform == 'linux':
    global_font_path = '/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf'

def plot_layout_image(images, layout_df):

    if len(images) != len(layout_df['page_index'].unique()):
        raise ValueError('The number of images and pages should be the same.')
        
    res_imgs = []
    
    for page_index, image in enumerate(images):
        page_df = layout_df[layout_df['page_index'] == page_index]
        image = images[page_index]
        temp_res = add_label_blocks(
            image, 
            list(page_df['bbox']),
            list(page_df['label']),
            width=2, font_size=12
        )
        res_imgs.append(temp_res)
        
    return res_imgs

def add_label_blocks(image, bboxes, labels, color="red", width=2, font_size=10):
    '''
    Add rectangles to the image.

    Args:
    image (PIL.Image): The image.
    bboxes (list): A list of bounding boxes.
    color (str): The color of the rectangles.
    width (int): The width of the rectangles.

    Returns:
    image (PIL.Image): The image with rectangles.
    '''
    image = image.copy()
    if len(bboxes) != len(labels):
        raise ValueError('The number of bounding boxes and labels should be the same.')
    
    draw = ImageDraw.Draw(image)
    for i in range(len(bboxes)):
        bbox = bboxes[i]
        draw.rectangle(bbox, outline=color, width=width)
        
        label = labels[i]
        font = ImageFont.truetype(global_font_path, font_size)
        draw.text((bbox[2] - font_size * 8, bbox[1]), label, font=font, fill=color)
        
    return image

def plt2pil(plt):
    '''
    Transform a matplotlib figure to a PIL image.
    '''
    plt.canvas.draw()
    data = np.frombuffer(plt.canvas.tostring_rgb(), dtype=np.uint8)
    data = data.reshape(plt.canvas.get_width_height()[::-1] + (3,))
    pil_image = Image.fromarray(data)
    return pil_image

def display_images(images, indices=None, scale=1):
    '''
    Display images in a grid.
    
    Parameters:
    images (list): A list of PIL images.
    col (int): The number of columns.
    indices (list): A list of indices of the images to display.
    '''
    # If not specified, display all images
    if indices is None:
        indices = range(len(images))
        
    col_num=math.ceil(math.sqrt(len(indices)))
        
    # Calculate the number of rows
    image_num = len(indices)
    row_num = (image_num + col_num - 1) // col_num
    
    # Compute the size of the grid
    max_width = max(image.width for image in images)
    max_height = max(image.height for image in images)
    totl_width = max_width * col_num
    totl_height = max_height * row_num
    
    # Create the figure
    fig, axes = plt.subplots(row_num, col_num, figsize=(totl_width / 100 * scale, totl_height / 100 * scale))
    
    # Adjust axes
    if row_num == 1:
        axes = [axes]
    if col_num == 1:
        axes = [[ax] for ax in axes]
        
    # Iterate over the indices
    for i, index in enumerate(indices):
        row = i // col_num
        col = i % col_num
        ax = axes[row][col]
        ax.imshow(images[index])
        ax.axis('off')
        
        # Add a border around the image
        temp_h, temp_w = images[index].size
        rect = Rectangle((0, 0), temp_h, temp_w, edgecolor='black', facecolor='none', linewidth=3)
        ax.add_patch(rect)
        
    # Hide the empty axes
    for i in range(image_num, row_num * col_num):
        row = i // col_num
        col = i % col_num
        axes[row][col].axis('off')
        
    plt.tight_layout()
    
    # Return in PIL Image format
    return plt2pil(fig)

# OCR analysis

def check_ocr(images, layout_row, ppi=72):
    cropped = get_cropped_image(images, layout_row)
    ocr_img = get_bbox_ocr_res(layout_row)
    
    # Resize cropped image
    cropped = cropped.resize((ocr_img.width, ocr_img.height))

    # Concatenate the two images horizontally
    concat_img = Image.new('RGB', (cropped.width + ocr_img.width, max(cropped.height, ocr_img.height)))
    concat_img.paste(cropped, (0, 0))
    concat_img.paste(ocr_img, (cropped.width, 0))
    
    # Add a red vertical line to separate the two images
    draw = ImageDraw.Draw(concat_img)
    draw.line((cropped.width, 0, cropped.width, max(cropped.height, ocr_img.height)), fill='red', width=2)

    return concat_img

def get_cropped_image(images, layout_row):

    temp_row = layout_row
    page_index = temp_row['page_index']
    bbox = temp_row['bbox']

    # Get the image
    image = images[page_index]
    cropped = image.crop(bbox)
    
    return cropped

def get_bbox_ocr_res(layout_row):

    text = layout_row['text']
    bbox = layout_row['bbox']

    # Scale the canvas, be same as the cropped image
    canvas_width = bbox[2] - bbox[0]
    canvas_height =bbox[3] - bbox[1]

    font_path = global_font_path

    return draw_text_on_canvas(text, canvas_width, canvas_height, font_path)

def draw_text_on_canvas(text, canvas_width, canvas_height, font_path):
    
    font_size = get_best_font_size(text, canvas_width, canvas_height)
    # 创建空白画布
    image = Image.new('RGB', (canvas_width, canvas_height), color=(255, 255, 255))
    draw = ImageDraw.Draw(image)
    
    # 设置字体
    font = ImageFont.truetype(font_path, font_size)
    
    # 初始化文本位置
    x, y = 2, 2  # 可以根据需要调整文本起始位置
    
    # 获取文本的最大宽度
    max_width = canvas_width - 10  # 保持一定边距
    
    # 按照指定宽度自动换行
    lines = []
    words = text.split()
    line = []
    for word in words:
        line.append(word)
        w = len(' '.join(line)) * font_size / 2.1
        if w > max_width:
            line.pop()
            lines.append(' '.join(line))
            line = [word]
    lines.append(' '.join(line))
    
    # 将每一行画到画布上
    h = font_size * 1.5
    for line in lines:
        draw.text((x, y), line, font=font, fill=(0, 0, 0))
        y += h
    
    return image

def get_best_font_size(text, canvas_width, canvas_height):
    def text_fits(font_size):
        max_width = canvas_width - 10
        
        # 按照指定宽度自动换行
        lines = []
        words = text.split()
        line = []
        for word in words:
            line.append(word)
            w = len(' '.join(line)) * font_size / 2.1  # 假设每个字符的平均宽度为 font_size / 2.1
            if w > max_width:
                line.pop()
                lines.append(' '.join(line))
                line = [word]
        lines.append(' '.join(line))
        
        # 将每一行画到画布上
        h = font_size * 1.5  # 行高，假设为 font_size 的 1.5 倍
        
        # 判断总高度是否适合画布
        return h * len(lines) <= canvas_height

    # 初始化二分查找范围
    low, high = 1, 100
    best_size = 0
    
    while low <= high:
        mid = (low + high) // 2
        
        if text_fits(mid):
            best_size = mid  # 当前字体大小合适
            low = mid + 1    # 尝试更大的字体
        else:
            high = mid - 1   # 尝试更小的字体
    
    return best_size - 1

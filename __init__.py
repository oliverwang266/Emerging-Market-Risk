# Standard library imports
import sys
from pathlib import Path

# Add the path to the sys.path
ocr_path = Path(__file__).parent
temp_folder = ocr_path / 'temp'
sys.path.append(str(ocr_path))

# Third party imports
import layout_surya as ls
import ocr_tesseract as ot
import toolkit as tk


# Get the localtion of this py script
ocr_path = Path(__file__).parent
temp_folder = ocr_path / 'temp'

if not temp_folder.exists():
    temp_folder.mkdir()

class PdfParser:
    def __init__(self, tesseract_cmd='tesseract'):
        print("Initializing Docment Parser......")
        print("Loading Surya models......")
        self.surya_models = ls.load_surya_models()
        print("Initializing Tesseract......")
        self.tesseract_cmd = tesseract_cmd
    
    def parse_pdf(self, pdf_bytes, lang='eng', ppi=150, temp_name='sample'):
        '''
        Parse a pdf document and return a DataFrame with the following columns,
        input support pdf path, pdf bytes, list of Pillow images, and fitz.Document
        '''

        surya_df = self.get_surya_df(pdf_bytes, ppi=ppi)
        ocr_df = self.get_ocr_df(pdf_bytes, surya_df, lang=lang, surya_ppi=ppi, ocr_ppi=ppi, temp_name=temp_name)

        # When running the OCR, bbox in the layout_df will be scaled to the OCR image size
        # and add a buffer of 2 * scale pixels on each side
        
        merged_df = self._merge_layout_ocr(surya_df, ocr_df)
        # Rename page_idx to page_index
        merged_df.rename(columns={'page_idx': 'page_index'}, inplace=True)
        merged_df = merged_df[['page_index', 'position', 'bbox', 'label', 'text', 'layout_ppi']]
        merged_df.reset_index(drop=True, inplace=True)

        return merged_df
    
    def get_surya_df(self, pdf_bytes, ppi=72):
        layout_images = tk.pdfbytes2imgs(pdf_bytes, ppi=ppi)
        surya_df = ls.run_surya(
            images=layout_images,
            det_model=self.surya_models['det_model'],
            det_processor=self.surya_models['det_processor'],
            layout_model=self.surya_models['layout_model'],
            layout_processor=self.surya_models['layout_processor'],
            ordering_model=self.surya_models['ordering_model'],
            ordering_processor=self.surya_models['ordering_processor'],
        )
        surya_df['layout_ppi'] = ppi
        return surya_df
    
    def get_ocr_df(self, pdf_bytes, surya_df, lang='eng', surya_ppi=72, ocr_ppi=150, temp_name='sample'):
        ocr_images = tk.pdfbytes2imgs(pdf_bytes, ppi=ocr_ppi)
        ocr_images = [tk.preprocess_img(img) for img in ocr_images]
        if surya_ppi == ocr_ppi:
            buffer_size = 0
        else:
            buffer_size = 2
        print("Running OCR......")
        ocr_df = ot.run_ocr(images=ocr_images, layout_table=surya_df, image_ppi=ocr_ppi, layout_ppi=surya_ppi, lang=lang, temp_path=temp_folder / f'{temp_name}.tiff', buffer_size=buffer_size)
        return ocr_df

    
    def _merge_layout_ocr(self, layout_df, ocr_df):
        # Add unique_id and create a map
        # Rename page_index to page_idx
        layout_df.rename(columns={'page_index': 'page_idx'}, inplace=True)
        ocr_df['unique_id'] = ocr_df['page_idx'].astype(str) + '_' + ocr_df['block_idx'].astype(str)
        layout_df['unique_id'] = layout_df['page_idx'].astype(str) + '_' + layout_df['block_idx'].astype(str)
        text_map = dict(zip(ocr_df['unique_id'], ocr_df['text']))

        # Add text to layout_df
        layout_df['text'] = layout_df['unique_id'].map(text_map)

        # Remove unique_id
        layout_df.drop(columns=['unique_id'], inplace=True)

        return layout_df

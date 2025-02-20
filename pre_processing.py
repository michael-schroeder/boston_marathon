import logging
import re
from typing import List

import pandas as pd
from pypdf import PdfReader

logger = logging.getLogger(__name__)
log_name = 'preprocess_log'
logging.basicConfig(encoding='utf-8', 
                    level=logging.INFO,
                    handlers=[
                        logging.FileHandler(log_name, mode='w'),
                        #logging.StreamHandler()
                    ])

def clean_spaces(phrases: List[str]):
    clean_phrases = [re.sub(' ([A-Z])', '||||\\1', phrase).replace(' ', '').replace('||||', ' ') for phrase in phrases]
    return clean_phrases

pdf_path = "BostonMarathonHistoricalResults.pdf"
reader = PdfReader(pdf_path)
page_count = 0
for i, page in enumerate(reader.pages):
    extracted_text = page.extract_text(extraction_mode="layout", layout_mode_space_vertically=False)
    # grabs out lines
    split_page = extracted_text.split('\n')
    # year, gender, time, first name, last name, state
    data = [re.split(r' {16,}', info) for info in split_page if len(re.split(r' {16,}', info)) > 5]
    if page_count == 0:
        headers = [header.replace(' ', '') for header in data[2]]
        df = pd.DataFrame(columns=headers)
        data = data[3:]
        page_count += 1
    # remove excess spaces in names
    clean_data = [clean_spaces(point) for point in data]
    try:
        df_page = pd.DataFrame(clean_data, columns=headers)
    except Exception as e:
        print(e)
        for j, val in enumerate(clean_data):
            if len(val) != 6:
                logger.info(f'page {i}')
                logger.info(e)
                logger.info(split_page[j])
                logger.info(data[j])
                logger.info(val)
                logger.info('---')

    df = pd.concat([df, df_page])
    if i % 50 == 0:
        print(f'On page {i}')

csv_path = "boston_marathon_2.csv" 
df.to_csv(csv_path, index=False)
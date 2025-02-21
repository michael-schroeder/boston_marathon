import concurrent.futures
import logging
import re
import time
from typing import List

import pandas as pd
from pypdf import PageObject, PdfReader

logger = logging.getLogger(__name__)
log_name = 'preprocess_log_para.log'
logging.basicConfig(encoding='utf-8', 
                    level=logging.INFO,
                    handlers=[
                        logging.FileHandler(log_name, mode='w'),
                        #logging.StreamHandler()
                    ])


def clean_spaces(phrases: List[str]) -> List[str]:
    clean_phrases: List[str] = [re.sub(' ([A-Z])', '||||\\1', phrase).replace(' ', '').replace('||||', ' ') for phrase in phrases]
    return clean_phrases


def extract_text_from_pdf(page: PageObject, headers: List[str], contains_headers: bool=False) -> pd.DataFrame:
    extracted_text: str = page.extract_text(extraction_mode="layout", layout_mode_space_vertically=False)
    # grabs out lines
    split_page: List[str] = extracted_text.split('\n')
    # year, gender, time, first name, last name, state
    data: List[str] = [re.split(r' {16,}', info) for info in split_page if len(re.split(r' {16,}', info)) > 5]
    if contains_headers is True:
        headers: List[str] = [header.replace(' ', '') for header in data[0]]
        data: List[str] = data[1:]

    # remove excess spaces in names
    clean_data: List[str] = [clean_spaces(point) for point in data]
    try:
        df_page: pd.DataFrame = pd.DataFrame(clean_data, columns=headers)
    except Exception as e:
        print(e)
        for j, val in enumerate(clean_data):
            if len(val) != 6:
                logger.info(f'page {j}')
                logger.info(e)
                logger.info(split_page[j])
                logger.info(data[j])
                logger.info(val)
                logger.info('---')

    return df_page

## start
csv_path: str = "boston_marathon_para.csv" 
pdf_path: str = "BostonMarathonHistoricalResults.pdf"
reader: PdfReader = PdfReader(pdf_path)
header_page: PageObject = reader.pages[0]
df: pd.DataFrame = extract_text_from_pdf(header_page, headers=[], contains_headers=True)
headers: List[str] = df.columns.to_list()
b = 1

start_time = time.time()
###
with concurrent.futures.ThreadPoolExecutor() as executor:
    # futures: List[pd.DataFrame] = [executor.submit(extract_text_from_pdf, page, headers) for page in reader.pages[1:10]]
    futures: List[pd.DataFrame] = [executor.submit(extract_text_from_pdf, page, headers) for page in reader.pages[1:]]
    for future in futures:
        df = pd.concat([df, future.result()])

endtime = time.time()
logger.info(f'took {round(endtime - start_time)} seconds')

df.to_csv(csv_path, index=False, header=True)

"""

###
for i, page in enumerate(reader.pages):
    extracted_text = page.extract_text(extraction_mode="layout", layout_mode_space_vertically=False)
    # grabs out lines
    split_page = extracted_text.split('\n')
    # year, gender, time, first name, last name, state
    data = [re.split(r' {16,}', info) for info in split_page if len(re.split(r' {16,}', info)) > 5]
    if page_count == 0:
        headers = [header.replace(' ', '') for header in data[0]]
        df = pd.DataFrame(columns=headers)
        data = data[1:]
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
    # progess tracker
    if i % 200 == 0:
        print(f'On page {i}')

endtime = time.time()
logger.info(f'took {round(endtime - start_time)} seconds')

df.to_csv(csv_path, index=False, header=True)
"""
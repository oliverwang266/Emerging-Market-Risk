# conda activate rcs_2023.09
# bsub -J "parse_pdf[1-3]" -q gpu -M 24G -n 6 -gpu - /usr/local/app/rcs_bin/grid3/envs/rcs_2023.09/bin/python /export/projects4/mmiller_emrisk/ocr_round_2/round2.py 3

from pdf_parser import PdfParser
import os
from report_db import ReportDatabase, ResultDatabase
import json
import pandas as pd
import sqlite3

# Get parent path
import sys
from pathlib import Path
file_path = Path(__file__).resolve()
parent_path = file_path.parent
sys.path.append(str(parent_path))

code_path = Path('/export/projects4/mmiller_emrisk/ocr_round_2')
temp_path = code_path / 'temp'
input_path = code_path / 'db_input'
output_path = code_path / 'db_output'

def main():
    job_num = int(sys.argv[1])
    print(f'Job number: {job_num}')
    job_id = int(os.environ['LSB_JOBINDEX'])
    print(f'Job ID: {job_id}')
    report_names = get_report_names(job_id, n_jobs=job_num)
    table_name = f'parsed_{job_id}'

    res_db = ResultDatabase(output_path / 'parsed.db')
    processed = set(res_db.get_report_names())

    parser = PdfParser()

    pdf_db = ReportDatabase(input_path / 'report_db.db')

    for r in report_names:

        temp = pdf_db.get_report(r)
        report_name = temp['report_name']
        source = temp['source']
        pdf = temp['pdf']

        if report_name in processed:
            print(f'{report_name} already processed.')
            continue

        df = parser.parse_pdf(pdf, ppi=150, temp_name=report_name)

        # Bounding box to text
        df['bbox'] = df['bbox'].apply(lambda x: json.dumps(x))

        df['report_name'] = report_name
        df['source'] = source

        res_db.insert_df(table_name, df)

def get_report_names(job_id, n_jobs=3):
    with open(temp_path / 'report_names.json', 'r') as f:
        report_names = json.load(f)

    batch_size = len(report_names) // n_jobs
    start = (job_id - 1) * batch_size
    end = job_id * batch_size
    if job_id == n_jobs:
        end = len(report_names)

    report_names = report_names[start:end]
    return report_names

if __name__ == '__main__':
    main()


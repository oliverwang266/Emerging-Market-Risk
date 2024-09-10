import subprocess
import os
import zipfile
import py7zr
from pathlib import Path
import sqlite3
import pandas as pd
from tqdm import tqdm
import shutil
import json
from report_db import ReportDatabase
import random

raw_reports_dropbox = Path('/Finance and Development/data/raw/reports')
raw_reports = Path('/export/projects4/mmiller_emrisk/raw_reports')

oxford = 'oxford_economic_reports'
prs = 'political_risk_service'

oxford_path = raw_reports / oxford / 'orig'
prs_path = raw_reports / prs / 'orig'

code_path = Path('/export/projects4/mmiller_emrisk/ocr_round_2')

# Copy reports from Dropbox to the local machine
def sync_reports():
    def copy_from_dropbox(src, dest, ignore_existing=True):
        src = str(src)
        dest = str(dest)
        folder_name = src.split('/')[-1]
        if " " in src:
            src = '"' + src + '"'
        cmd = f'rclone -P sync Dropbox:{src} {dest}/{folder_name}'
        if ignore_existing:
            cmd += ' --ignore-existing'
        subprocess.run(cmd, shell=True, check=True)

    oxford_src = raw_reports_dropbox / oxford
    prs_src = raw_reports_dropbox / prs

    copy_from_dropbox(oxford_src, raw_reports)
    copy_from_dropbox(prs_src, raw_reports)

# Navigate through the folders
# Build database for the reports


if __name__ == '__main__':
    # sync_reports()

    # Save the pdf to the database
    report_db = ReportDatabase('report_db.db')
    added_reports = report_db.get_report_names()
    print(f'{len(added_reports)} reports already exist in the database.')

    # If not all reports are added to the database
    if len(added_reports) != 15413:
        # OXFORD
        print(os.listdir(oxford_path))
        # '2022.07.01-2024.07.29'
        for r in tqdm(os.listdir(oxford_path / '2022.07.01-2024.07.29')):
            if not r.endswith('.pdf'):
                print(r)
                raise ValueError
            report_name = r.removesuffix('.pdf')
            if report_name in added_reports:
                # print(f'{report_name} already exists in the database.')
                continue
            pdf_bytes = open(oxford_path / '2022.07.01-2024.07.29' / r, 'rb').read()
            source = 'oxford'
            report_db.insert_report(report_name, source, pdf_bytes)

        # 'OxfordEconomics Reports 10.29.2013-7.29.202420240730071455.zip'
        # Navigate through the zip files
        with zipfile.ZipFile(oxford_path / 'OxfordEconomics Reports 10.29.2013-7.29.202420240730071455.zip', 'r') as z:
            for r in z.namelist():
                print("Unzipping", r)
                if not r.endswith('.zip'):
                    raise ValueError
                with zipfile.ZipFile(z.open(r), 'r') as z2:
                    for r2 in tqdm(z2.namelist()):
                        if not r2.endswith('.pdf'):
                            raise ValueError
                        report_name = r2.removesuffix('.pdf')
                        if report_name in added_reports:
                            # print(f'{report_name} already exists in the database.')
                            continue
                        pdf_bytes = z2.read(r2)
                        source = 'oxford'
                        report_db.insert_report(report_name, source, pdf_bytes)

        # POLITICAL RISK SERVICE
        print(os.listdir(prs_path))
        # prs.zip
        with zipfile.ZipFile(prs_path / 'prs.zip', 'r') as z:
            for r in tqdm(z.namelist()):
                if not r.endswith('.pdf'):
                    raise ValueError
                report_name = r.removesuffix('.pdf')
                if report_name in added_reports:
                    # print(f'{report_name} already exists in the database.')
                    continue
                pdf_bytes = z.read(r)
                source = 'prs'
                report_db.insert_report(report_name, source, pdf_bytes)

        # FW_Political Risk Service_PRS country reports20240529021516.zip
        with zipfile.ZipFile(prs_path / 'FW_Political Risk Service_PRS country reports20240529021516.zip', 'r') as z:
            for r in z.namelist():
                if not r.endswith('.7z'):
                    raise ValueError
                with py7zr.SevenZipFile(z.open(r), 'r') as z2:
                    filenames = z2.getnames()
                    # Create a temp folder to extract the files if needed
                    prs_path.mkdir(exist_ok=True)
                    z2.extractall(prs_path / 'temp')
                    for r2 in filenames:
                        if r2.endswith('.pdf'):
                            report_name = r2.split('/')[-1].removesuffix('.pdf')
                            pdf_bytes = open(prs_path / 'temp' / r2, 'rb').read()
                            source = 'prs'
                            report_db.insert_report(report_name, source, pdf_bytes)
                        else:
                            print(f'Not a pdf: {r2}')
                    # Clean temp folder wit shutil.rmtree
                    shutil.rmtree(prs_path / 'temp')

        report_db.close()

    # Get the unique report names and save them to a json file
    report_names = report_db._get_unique_report_names()
    print(f'{len(report_names)} reports in total.')
    report_names = list(report_names)
    random.seed(42)
    random.shuffle(report_names)

    with open(code_path / 'temp' / 'report_names.json', 'w') as f:
        json.dump(report_names, f, indent=4)
    
    pass
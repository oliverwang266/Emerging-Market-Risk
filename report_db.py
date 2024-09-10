import sqlite3
import pandas as pd

class ReportDatabase:
    def __init__(self, db_path):
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        self.report_names = set(self._get_unique_report_names())

        # Create table for reports if not exists
        # report_name, source, pdf
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS reports (
                report_name TEXT PRIMARY KEY,
                source TEXT,
                pdf BLOB
            )
        ''')
        self.conn.commit()

    def get_report(self, report_name):
        self.cursor.execute('''
            SELECT * FROM reports WHERE report_name = ?
        ''', (report_name,))
        temp = self.cursor.fetchone()
        res = {
            'report_name': temp[0],
            'source': temp[1],
            'pdf': temp[2]
        }
        return res

    def insert_report(self, report_name, source, pdf_bytes):
        try:
            self.cursor.execute('''
                INSERT INTO reports (report_name, source, pdf) VALUES (?, ?, ?)
            ''', (report_name, source, pdf_bytes))
            self.conn.commit()
        except sqlite3.IntegrityError: # If the report_name duplicates in one run......
            print(f'{report_name} already exists in the database.')

    def get_report_names(self):
        return self.report_names

    def _get_unique_report_names(self):
        self.cursor.execute('''
            SELECT DISTINCT report_name FROM reports
        ''')
        return [r[0] for r in self.cursor.fetchall()]

    def close(self):
        self.conn.close()

class ResultDatabase():
    def __init__(self, db_path):
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        self.table_names = self._get_table_names()
        self.table_reports_dict = {table_name: set(self._get_report_names(table_name)) for table_name in self.table_names}
        self.all_report_names = set([report_name for table_name in self.table_names for report_name in self.table_reports_dict[table_name]])

    def get_table_names(self):
        return self.table_names
    
    def get_report(self, report_name):
        for table_name in self.table_names:
            if report_name in self.table_reports_dict[table_name]:
                return pd.read_sql_query(f'SELECT * FROM {table_name} WHERE report_name = "{report_name}"', self.conn)
        print(f'Report {report_name} not found')
        return None
        
    def get_report_names(self):
        return self.all_report_names
    
    def insert_df(self, table_name, df):
        # Check if report_name already exists
        report_name = df['report_name'].iloc[0]
        if report_name in self.all_report_names:
            print(f'Report {report_name} already exists. Skipping...')
        else:
            df.to_sql(table_name, self.conn, if_exists='append', index=False)
            self.table_reports_dict[table_name].add(report_name)
            self.all_report_names.add(report_name)

    def _get_table_names(self):
        return pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table';", self.conn)['name'].tolist()

    def _get_report_names(self, table_name):
        return pd.read_sql_query(f'SELECT DISTINCT report_name FROM {table_name}', self.conn)['report_name'].tolist()
    
    def _get_all_report_names(self):
        report_names = []
        for table_name in self.table_names:
            report_names += self._get_report_names(table_name)
        return report_names

    def close(self):
        self.conn.close()
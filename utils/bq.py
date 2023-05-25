import json
import logging
import random
import re
import string
import traceback
from datetime import date, datetime
import pandas as pd

import pandas_gbq as pgbq
from google.cloud import bigquery
from google.oauth2 import service_account


class GoogleBigQuery:
    def __init__(self, project, dataset, service_account_file_path) -> None:
        self.project_name = project
        self.dataset_name = dataset
        self.client = bigquery.Client()
        self.credentials = service_account.Credentials.from_service_account_file(
            service_account_file_path
        )

    def execute_stored_procedure(self, stored_proc_name):
        query = (
            f"""CALL `{self.project_name}.{self.dataset_name}.{stored_proc_name}`()"""
        )
        # print(query)
        df = self.client.query(query).to_dataframe()
        return df

    def insert_alter(self, table, df, mode="append", stored_proc=None):
        """
        table :  str - target table
        df : pandas.DataFrame - data to be ingested
        mode : str - mode to upload the data
            mode = append : this will append data to existing schema
            mode = truncate : truncate table and upload data
            mode = replace : upsert merge data into existing schema based on the match conditions in stored procedure
        stored_proc : stored proc to run in case or `replace` mode
        """
        dataset = self.dataset_name

        def column_mapper(x):
            """Standardises column name"""
            return re.sub(r"[^a-zA-Z0-9_]", "_", x)

        if len(df) == 0:
            logging.log(25, f"empty df nothing to insert")
            return
        if mode == "replace" and stored_proc is None :
            raise Exception("empty stored_proc received for mode replace")
        elif mode not in ["append", "replace", "truncate"]:
            raise Exception("mode can be only 'append'/'replace'/'truncate'")

        def mapper(x):
            if type(x) is None or str(x).lower() == "nan":
                ret = None
            elif type(x) == str:
                ret = x.replace("\x00", " ")
            elif type(x) == date or type(x) == datetime:
                ret = str(x).replace("\x00", " ")
            elif isinstance(x, (list, dict)):
                ret = json.dumps(x).replace("\x00", " ")
            else:
                ret = str(x).replace("\x00", " ")
            return ret

        df_copy = df.applymap(mapper)

        # comparing column names for input df and source table in BQ
        df_copy.columns = map(column_mapper, df_copy.columns)
        columns_input_df = df_copy.columns
        sc = self.get_schema(dataset, table)
        if len(sc) > 0 and mode != "truncate":
            columns_source_table = [s["name"] for s in sc]
            cols_to_add_to_source_table = list(
                set(columns_input_df) - set(columns_source_table)
            )
            if len(cols_to_add_to_source_table) > 0:
                query = f"alter table {dataset}.{table} "
                query += ",".join(
                    [
                        f"add column if not exists {col} string "
                        for col in cols_to_add_to_source_table
                    ]
                )
                res = list(self.client.query(query).result()) == list()
                logging.log(25, f"{query} result {res}")

        if mode == "append" or len(sc) == 0:
            pgbq.to_gbq(
                df_copy,
                f"{dataset}.{table}",
                self.project_name,
                credentials=self.credentials,
                if_exists="append",
            )
        elif mode == "replace":
            tbl = "_STG_" + "".join(
                random.choice(string.ascii_uppercase) for _ in range(8)
            )
            # TODO this create table query can be improved
            # TODO should this be a temp table
            res = self.client.query(
                f"create table {dataset}.{tbl}_{table} as "
                f"select * from {dataset}.{table} LIMIT 0"
            )
            res = list(res.result())
            logging.log(
                25, f"table {dataset}.{tbl}_{table} created. result {res==list()}"
            )
            try:
                # pushing incremental data to staging table
                pgbq.to_gbq(
                    df_copy,
                    f"{dataset}.{tbl}_{table}",
                    self.project_name,
                    credentials=self.credentials,
                    if_exists="replace",
                )

                # calling the stored procedure
                merge_query = (
                    f"CALL {dataset}.{stored_proc}('{table}','{tbl}_{table}', '{dataset}');"
                )
                res = list(self.client.query(merge_query).result()) == list()
                logging.log(25, f"{merge_query} result {res}")
            except:
                traceback.print_exc()
                raise Exception("Stop")
            finally:
                truncate_query = f"drop table {dataset}.{tbl}_{table}"
                res = list(self.client.query(truncate_query).result()) == list()
                logging.log(25, f"{truncate_query} result {res}")

        elif mode == "truncate":
            truncate_query = f"DELETE FROM {dataset}.{table} WHERE 1=1"
            logging.log(25, list(self.client.query(truncate_query).result()))
            pgbq.to_gbq(
                df_copy,
                f"{dataset}.{table}",
                self.project_name,
                credentials=self.credentials,
                if_exists="append",
            )
            logging.log(25, f"completed truncate insert to {dataset}.{table}")

    def get_schema(self, dataset, table):
        try:
            schema = self.client.get_table(f"{dataset}.{table}").schema
            return [{"name": s.name, "type": s.field_type} for s in schema]
        except:
            return []
        
    def run_query(self, query):
        df = self.client.query(query).to_dataframe()
        if df.empty:
            return pd.DataFrame()
        return df
        
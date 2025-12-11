import logging
import time

import pandas
import sqlalchemy as sa
import pandas as pd
from sqlalchemy.exc import NoSuchTableError
from src.connectors.config import Config

class OracleConnector():
    def __init__(self, config:Config):
        self.conn = sa.create_engine(f"oracle+oracledb://{config.oracle_user}:{config.oracle_pass.get_secret_value()}@{config.oracle_host_ip}:1521/?service_name={config.oracle_service}")
        self.inspector = sa.inspect(self.conn)
        self.ORACLE_DTYPES = {
            'symbol': sa.types.VARCHAR(20),
            'barsize': sa.types.VARCHAR(7),
            'datetime': sa.types.DATE(),
            'open': sa.types.FLOAT,
            'high': sa.types.FLOAT,
            'low': sa.types.FLOAT,
            'close': sa.types.BIGINT,
            'volume': sa.types.BIGINT,
            'barcount': sa.types.FLOAT,
            'wap': sa.types.FLOAT
        }

    def insert_into_table(self, df:pd.DataFrame, table_name: str, write_mode: str, primary_keys: [str]) -> None:
        """
        Used by main interface to insert df into oracle
        :param write_mode: ignore, upser and overwrite
        :param table_name:
        :param df: df to be inserted
        :return:
        """

        start_time = time.time()
        write_mode = write_mode.lower()
        if write_mode == 'ignore':
            self._df_to_oracle_insert_ignore(df, table_name)
        elif write_mode == 'upsert':
                self._df_to_oracle_upsert(df, table_name, primary_keys)
        elif write_mode ==  'overwrite':
            self._df_to_oracle_overwrite(df, table_name, primary_keys)
        else:
            raise Exception("Invalid write mode")

        end_time = time.time()
        elapsed_time = end_time - start_time
        # TODO log number of records  written
        # logging.info(f"Number of Records inserted oracle table {table_name}: {len(df.index)}")
        logging.info(f"Execution time: {elapsed_time:.4f} seconds")

    def drop_table_if_exists(self, table_name: str) -> None:
        """
        Uses reflection to drop a table if exists
        :param table_name:
        :return:
        """
        try:
            # Attempt to reflect the table.
            tbl = sa.Table(table_name.lower(), sa.MetaData(), autoload_with=self.conn) #SQLAlchemy's default behavior is to treat all-lowercase names as case-insensitive
            logging.info(f"Table '{table_name}' found. Dropping it now...")

            with self.connect() as conn:
                tbl.drop(conn)  # Drop table

            logging.info(f"Table '{table_name}' successfully dropped.")
        except NoSuchTableError:
            logging.warning(f"Warning did find '{table_name}' in schema")

    def sql(self, sql_query) -> pd.DataFrame:
        """
        Used to query db
        :param sql_query:
        :return:
        """

        df = pd.read_sql_query(sql_query, self.conn, parse_dates={"DATETIME": '%Y-%m-%d'})
        df.columns = df.columns.str.upper()
        return df

    def connect(self) -> sa.engine.Connection:
        return self.conn.connect()

    #-------------------------------------PRIVATE-----------------------------------------#

    def _df_to_oracle_overwrite(self, df: pandas.DataFrame, table_name: str, primary_keys: [str]) -> int:
        """
        Writes table to oracle / Will overwrite if there's anything of the same name
        :param sa_table_object:
        :return:
        """

        #1. Drop table if exists
        self.drop_table_if_exists(table_name)
        df = self._lowercase_col_df(df.copy())

        #2. convert df into sa table obj
        sa_type_dict = OracleConnector._df_to_sa_types(df)

            #Sets the pk if identified
        columns = []
        for col_name, sql_type in sa_type_dict.items():
            is_pk = (col_name.lower() in (s.lower() for s in primary_keys))
            columns.append(sa.Column(col_name, sql_type, primary_key=is_pk))

        # Define the temporary table using the inferred columns
        tbl = sa.Table(
            table_name,
            sa.MetaData(),
            *columns,  # Unpack the list of Column objects
        )

        # self.drop_table_if_exists(table_name)
        #3. Create table
        with self.connect() as conn:
            tbl.create(conn)
            logging.info(f"Table '{table_name}' structure created.")

            # Convert DataFrame to a list of dictionaries for bulk insertion
            # df = self._capatilize_df(df)
            data_to_insert = df.to_dict(orient='records')

            # Insert data_tools into the temporary table
            if data_to_insert:  # Only execute insert if there's data_tools
                conn.execute(sa.insert(tbl), data_to_insert)
                conn.commit()
            else:
                logging.info(f"Oracle table created {table_name}")


        return len(df.index) #return number of records inserted


    def _df_to_oracle_upsert(self, df:pd.DataFrame, table_name: str, primary_keys: [str]) -> int:
        """
        will insert and update any records based off pk
        :param df:
        :param table_name:
        :return:
        """
        #1 Insert df into a temp table
        temp_table_name = "temp_table_name".upper()
        self.drop_table_if_exists(temp_table_name)
        temp_table = self._df_to_oracle_overwrite(df, temp_table_name, primary_keys)

        merge_sql = self._create_merge_statement(temp_table_name, table_name, "upsert")
        logging.info(f"Executing MERGE statement:\n{merge_sql}")

        #3: Execute the MERGE statement
        try:
            with self.connect() as conn:
                conn.execute(sa.text(merge_sql))
                conn.commit()
            logging.info("MERGE statement executed successfully.")
        except Exception as e:
            logging.error(f"An unexpected error occurred during upsert: {e}")
            raise
        finally:
            self.drop_table_if_exists(temp_table_name)


    def _df_to_oracle_insert_ignore(self, df: pd.DataFrame, table_name: str, primary_keys: [str]) -> int:
        """
        will not insert any records that violate primary_id contraints
        :param df:
        :param table_name:
        :return:
        """
        # 1 Create temp table to merge
        temp_table_name = "temp_table_name".upper()
        self.drop_table_if_exists(temp_table_name)
        temp_table = self._df_to_oracle_overwrite(df, temp_table_name, primary_keys)

        # 2 Create merge statement
        merge_sql = self._create_merge_statement(temp_table_name, table_name, "ignore")
        logging.info(f"Executing MERGE statement:\n{merge_sql}")

        # 3: Execute the MERGE statement
        try:
            with self.connect() as conn:
                conn.execute(sa.text(merge_sql))
                conn.commit()
            logging.info("MERGE statement executed successfully.")
        except Exception as e:
            logging.error(f"An unexpected error occurred during insert/ignore: {e}")
            raise e
        finally:
            self.drop_table_if_exists(temp_table_name)

    def _create_merge_statement(self, src_table_name: str, tgt_table_name: str, mode: str) -> str:
        """
        Creates the merge sql text to execute
        :param src_table_name:
        :param tgt_table_name:
        :param mode: either upsert or ignore
        :return:
        """
        col_list = self._get_col_list(tgt_table_name)
        pk_col_list = self._get_pk(tgt_table_name)

        # Convert column names to uppercase for Oracle's default case sensitivity
        all_cols = [col.upper() for col in col_list]
        unique_keys_upper = [col.upper() for col in pk_col_list]

        # Columns for SET clause (for updates)
        set_clauses = []

        for col in all_cols:
            if col not in unique_keys_upper:
                set_clauses.append(f"T.{col} = S.{col}")
        set_clause_str = ", ".join(set_clauses)

        # Columns for INSERT clause
        insert_cols_str = ", ".join(all_cols)
        insert_values_str = ", ".join([f"S.{col}" for col in all_cols])

        # ON clause for matching
        on_clauses = [f"S.{key} = T.{key}" for key in unique_keys_upper]
        on_clause_str = " AND ".join(on_clauses)

        # update clause
        mode = mode.lower()
        if mode == "upsert":
            update_clause_str = f"""
                WHEN MATCHED THEN
                   UPDATE SET {set_clause_str}
                    """
        elif mode == "ignore":
            update_clause_str = ""
        else:
            raise Exception("Invalid mode: " + mode)

        merge_sql = f"""
               MERGE INTO {tgt_table_name.upper()} T
               USING {src_table_name.upper()} S
               ON ({on_clause_str})
               {update_clause_str}
               WHEN NOT MATCHED THEN
                   INSERT ({insert_cols_str})
                   VALUES ({insert_values_str})
               """

        return merge_sql

    # Smaller helper functions ----------------------------------------------------------------------------------------

    def _check_if_table_exists(self, table_name: str) -> bool:
        if self.inspector.has_table(table_name):
            return True
        return False


    def _get_table_object(self, table_name: str) -> sa.Table:
        metadata = sa.MetaData()
        return sa.Table(table_name.lower(), metadata, autoload_with=self.conn)

    # noinspection PyTypeChecker
    def _get_col_list(self, table_name: str) -> [str]:
        table = self._get_table_object(table_name)
        if table.columns:
            col_list = [col.name.upper() for col in table.columns]
        else:
            raise Exception("No Columns found in table")

        return col_list

    def _get_pk(self, table_name: str) -> [str]:
        table = self._get_table_object(table_name)
        if table.primary_key:
            pk_columns = [col.name.upper() for col in table.primary_key.columns]
        else:
            raise Exception("No primary keys found in table")

        return pk_columns

    def _lowercase_col_df(self, df: pd.DataFrame) -> pd.DataFrame:
        df.columns =  df.columns.str.lower()
        return df

    #----Static functions ---------------------------------------------------------------------------------------------

    @staticmethod
    def _df_to_sa_types(df: pd.DataFrame, default_string_length: int = 255) -> dict:
        df_sqlalchemy_types = {}
        for col_name, dtype in df.dtypes.items():
            if pd.api.types.is_integer_dtype(dtype):
                df_sqlalchemy_types[col_name] = sa.Integer
            elif pd.api.types.is_float_dtype(dtype):
                df_sqlalchemy_types[col_name] = sa.Float
            elif pd.api.types.is_bool_dtype(dtype):
                df_sqlalchemy_types[col_name] = sa.Boolean
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                df_sqlalchemy_types[col_name] = sa.DateTime
            else:  # Default to String for object/string types
                df_sqlalchemy_types[col_name] = sa.String(default_string_length)  # default to using max

        return df_sqlalchemy_types
















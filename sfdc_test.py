# %%
# pip install simple-salesforce
# https://pypi.org/project/simple-salesforce/
# https://salesforce.stackexchange.com/questions/120318/accessing-the-salesforce-api-for-the-first-time-using-python?newreg=0dfd87044b814f2c84ce2cd9ec5e0841

# https://developer.salesforce.com/blogs/2021/09/how-to-automate-data-extraction-from-salesforce-using-python
from simple_salesforce import Salesforce
import pandas as pd
import duckdb
import os
from dotenv import load_dotenv
import glob
from tqdm import tqdm

# %%
con = duckdb.connect("sfdc_dm.db")
con.install_extension("spatial")
con.load_extension("spatial")
con.sql("SET TimeZone='America/Los_Angeles'")
# %%
load_dotenv()
# %%
sfdc_username = os.environ["SFDC_USERNAME"]
sfdc_password = os.environ["SFDC_PASSWORD"]
sfdc_security_token = os.environ["SFDC_SECURITY_TOKEN"]
sf = Salesforce(
    username=sfdc_username,
    password=sfdc_password,
    security_token=sfdc_security_token,
    # domain="test",
)


# %%
def refresh_tgt_tables(tables: list | None = None):
    if tables is None or len(tables) == 0:
        tables = ["Account", "Address", "Contact"]
    for obj_name in tables:
        # obj_name = 'Account'
        table_name = f"tgt_{obj_name.lower()}"
        full_name = f"sf.{obj_name}"
        tgt_object = eval(full_name)
        names = []
        for field in tgt_object.describe()["fields"]:
            # print(field['name'])
            names.append(field["name"])
        # ','.join(names)
        df = pd.DataFrame(
            sf.query_all(f"""SELECT {','.join(names)} FROM {obj_name}""")["records"]
        )
        print(f"Dowloaded {df.shape[0]} records for {obj_name} ")
        if df.shape[0] == 0:
            continue
        con.sql(f"DROP table IF EXISTS {table_name}")
        con.sql(f"create table {table_name} as select * from df")
        report_file_path = os.path.join("data", f"{table_name}.xlsx")
        df.to_excel(report_file_path, index=False)


# refresh_tgt_tables(['Account'])
# refresh_tgt_tables([])


# %%
def refresh_src_tables():
    file_mask = os.path.join("data", "src*.xlsx")
    files = glob.glob(file_mask)
    # print( files)
    for source_file in files:
        # print(os.path.basename(os.path.splitext(source_file)[0]) )
        table_name = os.path.basename(os.path.splitext(source_file)[0]).lower()
        df = pd.read_excel(io=source_file, header=2)
        print(f"Read {df.shape[0]} records from {source_file}")
        if df.shape[0] == 0:
            continue
        con.sql(f"DROP table IF EXISTS {table_name}")
        con.sql(f"create table {table_name} as select * from df")


# refresh_src_tables()
# %%


def load_mapping():
    source_file = os.path.join("data", "mapping.xlsx")
    mapp = con.sql(
        f"""  SELECT * FROM st_read(
                '{source_file}',
                open_options=['HEADERS=FORCE', 'FIELD_TYPES=AUTO'])
        """
    )
    con.sql("DROP table IF EXISTS mapping")
    con.sql("create table mapping as select * from mapp")
    print(f"Loaded {len(mapp)} records from {source_file}")


# load_mapping()
# %%
def refresh_staging(
    obj_name: str = "Account",
    key_column: str = "AccountNumber",
    src_table="src_contractors",
):
    mapping_columns = con.sql(f"""SELECT 
            CASE 
            WHEN transform_action = 'copy' THEN concat(source_table,'."',source_column,'"')  
            WHEN transform_action = 'default' THEN concat('''',default_value,'''')
            WHEN transform_action = 'rule' THEN rule_expression  END col_content,
            target_field col_alias
            FROM mapping where transform_action IS NOT NULL
                AND  target_object = '{obj_name}' 
                AND  source_table = '{src_table}' 
                """).df()
    cols_ = []
    for _i, row in mapping_columns.iterrows():
        cols_.append(f"{row.col_content} AS {row.col_alias}")
    cols_.append(f" '{src_table}' AS source_table")
    stg_table = f"stg_{obj_name.lower()}"
    # src_table = con.sql(f"""SELECT MAX(source_table) source_table FROM mapping where transform_action IS NOT NULL
    #             AND  target_object = '{obj_name}'""").fetchall()[0][0]
    select_from_source = f"SELECT {','.join(cols_)} FROM {src_table}"
    if stg_table not in list(con.sql("SHOW ALL TABLES").df()["name"]):
        con.sql(f"DROP table IF EXISTS {stg_table}")
        con.sql(f"CREATE TABLE {stg_table} AS {select_from_source}")
        con.sql(f"ALTER TABLE {stg_table} ADD COLUMN id VARCHAR")
        con.sql(f"ALTER TABLE {stg_table} ADD COLUMN success BOOLEAN")
        con.sql(f"ALTER TABLE {stg_table} ADD COLUMN errors VARCHAR[]")
    else:
        con.sql(f"DELETE FROM {stg_table} WHERE source_table = '{src_table}' ")
        con.sql(f"INSERT INTO {stg_table} BY NAME {select_from_source}")
    con.sql(f"""UPDATE {stg_table}  SET id = tgt_{obj_name.lower()}.id 
            FROM tgt_{obj_name.lower()} 
            WHERE
            {stg_table}.{key_column} = tgt_{obj_name.lower()}.{key_column} """)
    if obj_name == "Account":
        stg_account_errors()
    print(f"Staging table {stg_table} is refreshed")


# refresh_staging()


# %%
def load_in_sfdc(
    obj_name: str = "Account",
    key_column: str = "AccountNumber",
    src_table: str = "src_contractors",
    batch_size: int = 1000,
):
    # key_column = "AccountNumber"
    # obj_name = "Account"
    # src_table = "src_contractors"
    stg_table = f"stg_{obj_name.lower()}"
    # batch_size = 100
    sql = f"""SELECT * EXCLUDE (id, success, errors, source_table) 
            FROM {stg_table} 
            WHERE id IS NULL AND source_table ='{src_table}'
            AND errors IS NULL
            LIMIT {batch_size}"""
    # print(sql)
    payload = con.sql(sql).df().to_dict(orient="records")

    for a in tqdm(payload, desc=f"Loading {obj_name} into Salesforce.com"):
        # print(a)
        try:
            result = sf.Account.create(a)
            # print(result)  
            update_sql = f"""UPDATE {stg_table} SET id = '{result['id']}' 
                , success = '{result['success']}'   
                , errors = '{result['errors']}' 
                WHERE {key_column} = '{a[key_column]}'  
                """
        except            Exception as err:
                # print(type(err))
                update_sql = f"""UPDATE {stg_table} SET   success = 'False'   
                , errors = ['{str(type(err)).replace("'",'')}' ]
                WHERE {key_column} = '{a[key_column]}'  
                """
                # print(update_sql)

      
        con.sql(update_sql)


load_in_sfdc()


# %%
def create_preload_reports():
    con.sql("""CREATE OR REPLACE VIEW preload_account as SELECT * from stg_account""")


# create_preload_reports()
# %%
def run_preload_reports():
    for view_name in list(
        con.sql("""SELECT * from information_schema.tables 
                WHERE table_type ='VIEW'""").df()["table_name"]
    ):
        report_file_path = os.path.join("data", f"{view_name}.xlsx")
        if os.path.exists(report_file_path):
            os.remove(report_file_path)
        # con.sql(
        #     f"""COPY (SELECT * from {view_name}) 
        #     TO '{report_file_path}' 
        #     WITH (FORMAT GDAL, DRIVER 'xlsx') """
        # )
        df = con.sql(            f"""SELECT * from {view_name}"""   ).df()
        df.to_excel(report_file_path, index= False)
        print(f"Created report {report_file_path}")


# %%
def stg_account_errors():
    sql = """update stg_account set errors = [dups.dup_number::varchar]
            FROM
            (with same_names as (
            SELECT
                Name
            FROM
                stg_account
            group by
                all
            HAVING
                count(1)>1) 
            select
                ss.Name,
                ss.AccountNumber,
                ROW_NUMBER() over( PARTITION by ss.Name
            order by ss.id desc,
                ss.AccountNumber) dup_number
            from
                stg_account ss
            join same_names sn on
                ss.Name = sn.Name) dups
            where dups.AccountNumber = stg_account.AccountNumber
            and dups.dup_number >1 """
    con.sql(sql)
# %%
con.close()
# %%

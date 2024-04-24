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

# %%
con = duckdb.connect('sfdc_dm.db')
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
# sf.query_all("SELECT Id, Email FROM Contact")
# %%
names =[]
for field in sf.Account.describe()['fields']:
    # print(field['name'])
    names.append(field['name'])
','.join(names)
# %%
df = pd.DataFrame(sf.query_all(f"""SELECT {','.join(names)} FROM Account""")['records'])
# %%

con.sql("DROP table IF EXISTS tgt_account")
con.sql("create table tgt_account as select * from df")
# %%
report_file_path = os.path.join('data','tgt_account.xlsx')
df.to_excel(report_file_path, index=False )
# %%
target_account  = sf.query_all(f"""SELECT {','.join(names)} FROM Account""")['records']

# %%
con.sql("SELECT * FROM df").show()
# %%
source_file = os.path.join('data','src_contractors.xlsx')
contractors = pd.read_excel(io=source_file, header=2 )
con.sql("DROP table IF EXISTS src_contractors")
con.sql("create table src_contractors as select * from contractors")
# %%
con.sql("""SELECT "Contractor ID" AccountNumber,"Contractor Name" Name,"PO Box","Postal","City","Street","State Abbreviation","Zip Code" FROM src_account limit 10""").show()
# %%

','.join([f'"{col}"' for col in list(contractors.columns)])


# %%

source_file = os.path.join('data','mapping.xlsx')
mapp = duckdb.sql(
    f"""
        SELECT * FROM st_read(
            '{source_file}',
            open_options=['HEADERS=FORCE', 'FIELD_TYPES=AUTO'])
    """
)
con.sql("DROP table IF EXISTS mapping")
con.sql("create table mapping as select * from mapp")
# %%

# %%
mapping_columns = con.sql("""select 
        CASE 
        WHEN transform_action = 'copy' THEN concat(source_table,'."',source_column,'"')  
        WHEN transform_action = 'default' THEN concat('''',default_value,'''')
        WHEN transform_action = 'rule' THEN rule_expression  END col_content,
        target_field col_alias
         from mapping where transform_action is not null""").df()
# %%
cols_=[]
for _i, row in mapping_columns.iterrows():
    # print( row.col_content, row.col_alias)
    cols_.append(f'{row.col_content} AS {row.col_alias}')
','.join(cols_)

# %%
sql = f"SELECT {','.join(cols_)} FROM src_contractors limit 10"
# %%
account_payload = con.sql(sql).df().to_dict(orient='records')
# %%
for a in account_payload:
    print (a)
    result = sf.Account.create(a)
    print(result)
# %%
con.close()
# %%

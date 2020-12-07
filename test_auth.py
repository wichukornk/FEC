import pandas as pd
import pandas_gbq
from google.oauth2 import service_account
credentials = service_account.Credentials.from_service_account_file(
   "C:/Users/wichu/Documents/GitHub/FEC/IntroCloud01-7b6a00779302.json",
)

pandas_gbq.context.credentials = credentials

sql = """
SELECT country_name, alpha_2_code
FROM [bigquery-public-data:utility_us.country_code_iso]
WHERE alpha_2_code LIKE 'Z%'
LIMIT 100
"""
project_id="introcloud01"

df = pandas_gbq.read_gbq(
    sql,
    project_id=project_id,
    dialect="legacy", 
    #credentials=credentials
)

print(df.head())
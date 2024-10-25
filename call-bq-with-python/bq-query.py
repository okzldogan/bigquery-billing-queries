from google.cloud import bigquery

bq_project_id = "sp-billing-export"
client = bigquery.Client(project=bq_project_id)

project_name = "MY_PROJECT_NAME"
namespace_name = "MY_NAMESPACE_NAME"
dataset_name = "MY_DATASET_NAME"
billing_account_id = "MY_BILLING_ACCOUNT_ID"
billing_month = "02"

QUERY_FILE = "MY_QUERY_FILE.sql"

with open(QUERY_FILE, "r") as file:
    QUERY = file.read()
    # Set the string replacements in the SQL file
    QUERY = QUERY.replace("DATASET_PROJECT", project_name)
    QUERY = QUERY.replace("DATASET_NAME", dataset_name)
    QUERY = QUERY.replace("BILLING_ACCOUNT_ID", billing_account_id)
    QUERY = QUERY.replace("NAMESPACE-NAME", namespace_name)
    QUERY = QUERY.replace("BILLING-MONTH", billing_month)

job = client.query(QUERY)

for row in job.result():
    print("environment:" + row[2],"application:" + row[1],"cost:", + row[0])

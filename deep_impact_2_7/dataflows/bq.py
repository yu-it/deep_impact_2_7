from google.cloud import bigquery



def selectFromBq(query):
    client = bigquery.Client(project='yu-it-base')
    query_job = client.run_sync_query(query,)  # API request - starts the query
    query_job.run()
    return query_job.rows

from google.cloud import bigquery
from google.cloud import storage
def creating_partitioned_table(tableid,columnp,columnc):
    client = bigquery.Client()
    schema = [
    bigquery.SchemaField("trip_date", "DATE", mode="NULLABLE"),
    bigquery.SchemaField("trip_start_hour", "TIME", mode="NULLABLE"),
    bigquery.SchemaField("start_station_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("trip_count", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("total_trip_duration_minutes", "INTEGER", mode="NULLABLE")
    ]
    table = bigquery.Table(tableid, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
    type_=bigquery.TimePartitioningType.DAY,
    field=columnp,  # name of column to use for partitioning
    )
    table.clustering_fields=columnc
    table = client.create_table(table)
    print(
    "Created table {}, partitioned on column {}".format(
        table.table_id, table.time_partitioning.field
    )
    )
    return table

def create_table(tableid):
    client = bigquery.Client()
    schema = [
    bigquery.SchemaField("Title", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Genre", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Tags", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Languages", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Series_or_movie", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Hidden_gem_score", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("Country_available", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Runtime", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Director", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Writer", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Actors", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("View_rating", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("imdb_score", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("rotten_tomato_score", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("Metacric_Score", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("Awards_received", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("Awards_nominated", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("boxoffice", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Release_Date", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Netflix_releade_date", "DATE", mode="NULLABLE"),
    bigquery.SchemaField("Production_house", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("netflix_link", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("imdb_link", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("summary", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("votes", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("iamge", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("poster", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("tmdb_trailer", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("trailer_Site", "STRING", mode="NULLABLE"),
    ]
    table = bigquery.Table(tableid, schema=schema)
    table = client.create_table(table)
    print(
    "Created table")
    return table


def create_bucket(bucket_name):
    """
    Create a new bucket in the US region with the coldline storage
    class
    """

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    bucket.storage_class = "STANDARD"
    new_bucket = storage_client.create_bucket(bucket, location="us")

    print(
        "Created bucket {} in {} with storage class {}".format(
            new_bucket.name, new_bucket.location, new_bucket.storage_class
        )
    )
    return new_bucket

###create_bucket('nilay-pandey-netflix')
def load_data_using_query():
    client = bigquery.Client()

    job_config = bigquery.QueryJobConfig()
    # Set the destination table
    table_ref = client.dataset("nilay_bikeshare").table("hourly_summary_trip")
    job_config.destination = table_ref

    sql = """
        SELECT trip_date,trip_start_hour,start_station_name,trip_count,total_trip_duration_minutes from
        (select extract(date from start_time) as trip_date,extract(time from start_time) as trip_start_hour,
        start_station_name,count(start_station_name) as trip_count,
        sum(duration_minutes) as total_trip_duration_minutes 
        from `bigquery-public-data.austin_bikeshare.bikeshare_trips` group by start_station_name,extract(time from start_time),
        extract(date from start_time))
        """


# Start the query, passing in the extra configuration.
    query_job = client.query(sql, job_config=job_config)  # Make an API request.
    query_job.result()  # Wait for the job to complete.

    ###print("Query results loaded to the table {}".format(tableid))


###create_table("lateral-raceway-360606.nilay_netflix.nilay_netflix_raw_data")
def load_data_into_table(tableid,uri):
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
    schema = [
    bigquery.SchemaField("Title", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Genre", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Tags", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Languages", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Series_or_movie", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Hidden_gem_score", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("Country_available", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Runtime", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Director", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Writer", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Actors", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("View_rating", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("imdb_score", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("rotten_tomato_score", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("Metacric_Score", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("Awards_received", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("Awards_nominated", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("boxoffice", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Release_Date", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Netflix_releade_date", "DATE", mode="NULLABLE"),
    bigquery.SchemaField("Production_house", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("netflix_link", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("imdb_link", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("summary", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("votes", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("iamge", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("poster", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("tmdb_trailer", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("trailer_Site", "STRING", mode="NULLABLE"),
    ],
    skip_leading_rows=1,
    # The source format defaults to CSV, so the line below is optional.
    source_format=bigquery.SourceFormat.CSV,
    )
    load_job = client.load_table_from_uri(
    uri, tableid, job_config=job_config
    )
    load_job.result() 

"""create_table("lateral-raceway-360606.nilay_netflix.nilay_netflix_raw_data")
uri = "gs://nilay-pandey-netflix/netflix-rotten-tomatoes-metacritic-imdb.csv"
load_data_into_table("lateral-raceway-360606.nilay_netflix.nilay_netflix_raw_data",uri)"""
load_data_using_query()
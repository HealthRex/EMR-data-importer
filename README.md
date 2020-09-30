# EMR-data-importer

Pulls structured EMR data from an OMOP database using a given SQL query, and returns the results in a structured JSON file.

## Extracting labels

The JSON response contains a series of pairs of `label` and `data`. By default, the label is computed from the response dataframe to be the given row's column names. A custom script can also be passed in through the config file by specifying the name of the file (without the .py), which should contain a function called `get_row_labels` that accepts a pandas Series and returns a JSON-serializable object.

## Databases

### BigQuery

The config file credentials object should have two entries in it: `gcloud_project`, the project to connect to on Google Cloud Platform, and `gcloud_credentials`, the JSON credentials file that's generated when a user goes through the [authentication](https://cloud.google.com/docs/authentication) with Google Cloud Platform.

### Custom client

The Importer accepts a custom Client object, which should have a query function that takes in a SQL query and returns a pandas dataframe.
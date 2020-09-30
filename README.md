# EMR-data-importer

Pulls structured EMR data from an OMOP database using a given SQL query, and returns the results in a structured JSON file.

### BigQuery

The config file credentials object should have two entries in it: `gcloud_project`, the project to connect to on Google Cloud Platform, and `gcloud_credentials`, the JSON credentials file that's generated when a user goes through the [authentication](https://cloud.google.com/docs/authentication) with Google Cloud Platform.

### Custom client

The Importer accepts a custom Client object, which should have a query function that takes in a SQL query and returns a pandas dataframe.
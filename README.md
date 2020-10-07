# EMR-data-importer

Pulls structured EMR data from an OMOP database using a given SQL query, and returns the results in a structured JSON file.

## Extracting labels

The JSON response contains a series of pairs of `label` and `data`. By default, the label is computed from the response dataframe to be the given row's column names. A custom script can also be passed in through the config file by specifying the name of the file (without the .py), which should contain a function called `get_row_labels` that accepts a pandas Series and returns a JSON-serializable object.

## Databases

### BigQuery

The config file credentials object should have two entries in it: `gcloud_project`, the project to connect to on Google Cloud Platform, and `gcloud_credentials`, the JSON credentials file that's generated when a user goes through the [authentication](https://cloud.google.com/docs/authentication) with Google Cloud Platform.

### Custom client

The Importer accepts a custom Client object, which should have a query function that takes in a SQL query and returns a pandas dataframe.

## Examples

The following examples use the `bigquery-public-data` "Synthetic Patient Data in OMOP"
dataset provided by the [GCP Marketplace](https://console.cloud.google.com/marketplace/browse?filter=category:health). `bigquery-public-data` must be added to the BigQuery-enabled GCP project that the config file refers to.

Get the top N rows from any table:

```SELECT * FROM `bigquery-public-data.cms_synthetic_patient_data_omop.{table}` LIMIT {n}```

Join together three tables on `person_id` while selecting specific columns:

```
SELECT SELECT observation.*, condition.condition_type_concept_id, procedure.procedure_type_concept_id
FROM 
`bigquery-public-data.cms_synthetic_patient_data_omop.condition_occurrence` AS condition
INNER JOIN
`bigquery-public-data.cms_synthetic_patient_data_omop.observation_period` AS observation
ON
observation.person_id = condition.person_id
INNER JOIN
`bigquery-public-data.cms_synthetic_patient_data_omop.procedure_occurrence` AS procedure
ON procedure.person_id = observation.person_id
```

Suppose we wanted to take some of the values from the BigQuery results and train a model to predict patient observation days. For the query above, we can use the following functions for serializing the data:

```
def get_row_label(row):
    return (row.observation_period_end_date - row.observation_period_start_date).days

def get_row_value(row):
    columns = ['condition_type_concept_id', 'procedure_type_concept_id']
    return list(json.loads(row[columns].to_json()).values())
```

## Notes

When joining multiple tables, you will need to exclude duplicate columns (see [this StackOverflow question](https://stackoverflow.com/questions/53779191/bigquery-duplicate-column-names)). 
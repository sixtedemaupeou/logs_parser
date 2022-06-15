# logs_parser

This is a tool to process data.gouv.fr logs, compute the daily views of each page (resources, reuses, datasets and organizations are supported so far) with each access method (APIv1, APIv2, site).


## Running the tool

1. Start the database:
    - Run docker-compose up
    - Run the CLI command `logs_parser init_db --drop` to initialize the database tables.

2. Parse the logs and store the results in the DB.
    - Run the CLI command `logs_parser parse <logs_folder>` where `<logs_folder>` is the top level folder containing the logs within the dataeng bucket on Minio.
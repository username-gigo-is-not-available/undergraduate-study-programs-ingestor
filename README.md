# FCSE-Skopje 2023 Undergraduate Study Programs Ingestor

The Ingestor application is used to map and load from the preprocessing data of the undergraduate study programs and courses at
the [Faculty of Computer Science and Engineering](https://finki.ukim.mk) at
the [Ss. Cyril and Methodius University in Skopje](https://www.ukim.edu.mk).
which can be found at the following [URL](https://finki.ukim.mk/mk/dodiplomski-studii).

## Prerequisites

- Data from
  the [undergraduate-study-program-etl](https://github.com/username-gigo-is-not-available/undergraduate-study-programs-etl) is
  required to run this application.

## Overview

The Ingestor application is used to map and load the data from the etl application to nodes and relationships in a Neo4j database.
In order to represent the relationships between the nodes, the application uses the following data model.

### Data Model

#### Nodes

- `StudyProgram(uid, name, duration, url, courses)`: represents a study program
- `Course(uid, name_mk, name_en, prerequisite, taught_by)`: represents a course
- `Professor(uid, name)`: represents a professor

#### Relationships

- `Curriculum(level, type, semester, season, academic_year)`: connects a study program with a course
- `Prerequisite(type, number_of_courses)`: connects a course with another course
- `TaughtBy()`: connects a course with a professor

## Requirements

- Python 3.9 or later

## Environment Variables

Before running the ingestor, make sure to set the following environment variables:

- `FILE_STORAGE_TYPE`: the type of storage to use (either `LOCAL` or `MINIO`)
- `INPUT_FILE_NAME`: the name of the input file
- `MAX_WORKERS`: the maximum number of workers to use

##### Neo4j

###### Connection

- `DATABASE_HOST_NAME`: the hostname of the Neo4j database
- `DATABASE_NAME`: the name of the Neo4j database
- `DATABASE_USER`: the username of the Neo4j database
- `DATABASE_PASSWORD`: the password of the Neo4j database
- `DATABASE_PORT`: the port of the Neo4j database

###### Connection Pool

- `DATABASE_CONNECTION_ACQUISITION_TIMEOUT`: the timeout for acquiring a connection to the Neo4j database
- `DATABASE_CONNECTION_TIMEOUT`: the timeout for connecting to the Neo4j database
- `DATABASE_MAX_CONNECTION_LIFETIME`: the maximum lifetime of a connection to the Neo4j database
- `DATABASE_MAX_CONNECTION_POOL_SIZE`: the maximum size of the connection pool to the Neo4j database

###### Transactions

- `DATABASE_MAX_TRANSACTION_RETRY_TIME`: the maximum time to retry a transaction to the Neo4j database
- `DATABASE_RETRY_COUNT`: the number of times to retry a transaction to the Neo4j database
- `DATABASE_RETRY_DELAY`: the delay between retries of a transaction to the Neo4j database
- `DATABASE_RETRY_EXPONENT_BASE`: the base of the exponential backoff for retries of a transaction to the Neo4j database

##### If running the application with local storage:

- `INPUT_DIRECTORY_PATH`: the path to the directory where the input files are stored

##### If running the application with MinIO:

- `MINIO_ENDPOINT`: the endpoint of the MinIO server
- `MINIO_ACCESS_KEY`: the access key of the MinIO server
- `MINIO_SECRET_KEY`: the secret key of the MinIO server
- `MINIO_SOURCE_BUCKET_NAME`: the name of the bucket where the input files are stored

## Installation

1. Clone the repository
    ```bash
    git clone <repository_url>
    ```

2. Install the required packages
    ```bash
    pip install -r requirements.txt
    ```

3. Run the application
    ```bash
    python main.py
    ```

Make sure to replace `<repository_url>` with the actual URL of the repository.
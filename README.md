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

- `StudyProgram(uid, code, name, duration, url, offers(Course))`: represents a study program
- `Course(uid, code, name_mk, name_en, offered_by(StudyProgram), is_prerequisite_for(Course), has_prerequisite_for(Course), taught_by(Professor))`:
  represents a course
- `Professor(uid, name, surname, teaches(Course))`: represents a professor

#### Relationships

- `StudyProgramOffersCourse/CourseOfferedByStudyProgram(level, type, semester, semester_season, academic_year)`: connects a study program
  with a course
- `Prerequisite(type, number_of_courses)`: connects a course with another course
- `TaughtBy()`: connects a course with a professor

## Requirements

- Python 3.9 or later

## Environment Variables

Before running the ingestor, make sure to set the following environment variables:

- `FILE_STORAGE_TYPE`: the type of storage to use (either `LOCAL` or `MINIO`)

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

#### File Storage

##### If running the application with local storage:

- `INPUT_DIRECTORY_PATH`: the path to the directory where the input files are stored

##### If running the application with MinIO:

- `MINIO_ENDPOINT`: the endpoint of the MinIO server
- `MINIO_ACCESS_KEY`: the access key of the MinIO server
- `MINIO_SECRET_KEY`: the secret key of the MinIO server
- `MINIO_SOURCE_BUCKET_NAME`: the name of the bucket where the input files are stored

##### Dataset Paths

- `STUDY_PROGRAMS_INPUT_DATA_FILE_NAME`: the name of the file containing the study programs data
- `COURSES_INPUT_DATA_FILE_NAME`: the name of the file containing the courses data
- `PROFESSORS_INPUT_DATA_FILE_NAME`: the name of the file containing the professors data
- `CURRICULA_INPUT_DATA_FILE_NAME`: the name of the file containing the curricula data -
  this file contains the courses offered by each study program
- `COURSES_PREREQUISITES_INPUT_DATA_FILE_NAME`: the name of the file containing the courses prerequisites data -
  this file contains the courses that are prerequisites for other courses
- `TAUGHT_BY_INPUT_DATA_FILE_NAME`: the name of the file containing the courses taught by data -
  this file contains the courses that are taught by professors

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
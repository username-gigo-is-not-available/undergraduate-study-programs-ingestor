# FCSE-Skopje 2023 Undergraduate Study Programs Ingestor

The Ingestor application is used to map and load from the preprocessing data of the undergraduate study programs and courses at
the [Faculty of Computer Science and Engineering](https://finki.ukim.mk) at
the [Ss. Cyril and Methodius University in Skopje](https://www.ukim.edu.mk).
which can be found at the following [URL](https://finki.ukim.mk/mk/dodiplomski-studii).

## Prerequisites

- Data from
  the [undergraduate-study-program-validator](https://github.com/username-gigo-is-not-available/undergraduate-study-programs-validator) is
  required to run this application.

## Overview

The Ingestor application is used to map and load the data from the validator application to nodes and relationships in a Neo4j database.
In order to represent the relationships between the nodes, the application uses the following data model.

### Data Model

#### Nodes

- `StudyProgram(uid, code, name, duration, url, offers(Curriculum))`: represents a study program
- `Course(uid, code, name_mk, name_en, level, included_by(Curriculum), is_prerequisite_for(Requisite), is_postrequisite_for(Requisite), taught_by(Professor))`:
  represents a course
- `Professor(uid, name, surname, teaches(Course))`: represents a professor
- `Curriculum(uid, type, semester_season, academic_year, semester, offered_by(StudyProgram), includes(Course))`: represents a curriculum
- `Requisite(uid, type, minimum_number_of_courses_required, requires_prerequisite(Course), requires_postrequisite(Course))`: represents a requisite

#### Relationships

- `OFFERS`: connects a study program with a curriculum
- `INCLUDES`: connects a curriculum with a course
- `PREREQUISITES`: connects a course with a requisite
- `POSTREQUISITES`: connects a course with a requisite
- `TEACHES`: connects a professor with a course

### Partitioning Strategy 

To enable safe and efficient parallel ingestion of relationships,  each record is assigned a `partition_uid` based on the last 
character of the source and destination `UUID`s (e.g., "a-7"). These identifiers are then grouped into batches using a wrap-around strategy: 
for each batch `i`, we include all partition pairs (`k-j`) where `k = (i + j) % N` and `N` is the number of partitions.
This ensures that each batch accesses disjoint node subsets and reduces the likelihood of lock contention in Neo4j, 
especially for skewed datasets where many relationships target the same nodes.

### Pipeline:

#### Study Program:

##### Load

- Load the study programs data (output from the validator) with the following columns:
  `study_program_id`, `study_program_code`, `study_program_name`, `study_program_duration`, `study_program_url`

##### Rename

- Drop `study_program` prefix in all columns

##### Ingest

- Create study program nodes with columns (`uid`, `code`, `name`, `duration`, `url`)

#### Course:

##### Load

- Load the courses data (output from the validator) with the following columns:
  `course_id`, `course_code`, `course_name_mk`, `course_name_en`, `course_url`, `course_level`

##### Rename

- Drop `course` prefix in all columns

##### Ingest

- Create course nodes with columns (`uid`, `code`, `name_mk`, `name_en`, `url`, `level`)

#### Professor:

##### Load

- Load the professors data (output from the validator) with the following columns:
  `professor_id`, `professor_name`, `professor_surname`

##### Rename

- Drop `professor` prefix in all columns

##### Ingest

- Create professor nodes with columns (`uid`, `name`, `surname`)


#### Curriculum:

##### Load

- Load the curricula data (output from the validator) with the following columns:
  `curriculum_id`, `course_type`, `course_semester`, `course_semester_season`, `course_academic_year`
- 
##### Rename

- Drop `course` and `curriculum` prefix in all columns

##### Ingest

- Create curriculum nodes with columns (`uid`, `type`, `semester`, `semester_season`, `academic_year`)

#### Requisite:

##### Load

- Load the offers data (output from the validator) with the following columns:
  `requisite_id`, `course_prerequisite_type`, `minimum_required_number_of_courses`,

##### Rename

- Rename `requisite_id` to `uid`, `course_prerequisite_type` to `type`

##### Ingest

- Create requisite nodes with columns (`uid`, `type`, `minimum_required_number_of_courses`)

#### Offers:

##### Load

- Load the teaches data (output from the validator) with the following columns:
  `offers_id`, `curriculum_id`, `study_program_id`

##### Rename

- Rename `offers_id` to `uid`

##### Partition 

- Generate `partition_uid` by extracting the last characters from `curriculum_id` and `study_program_id` 
  and concatenating them with `-`
- Generate partition matrix by using the wrap around technique and partition the dataframe

##### Ingest

- For each partition, create `OFFERS` relationships from `StudyProgram` nodes to `Curriculum` nodes 
  by matching on the `uid` column

#### Includes:

- Load the teaches data (output from the validator) with the following columns:
  `includes_id`, `curriculum_id`, `course_id`

##### Rename

- Rename `includes` to `uid`

##### Partition 

- Generate `partition_uid` by extracting the last characters from `curriculum_id` and `course_id` 
  and concatenating them with `-`
- Generate partition matrix by using the wrap around technique and partition the dataframe

##### Ingest

- For each partition, create `INCLUDES` relationships from `Curriculum` nodes to `Course` nodes 
  by matching on the `uid` column

#### Prerequisites:

##### Load

- Load the prerequisite data (output from the validator) with the following columns:
  `prerequisite_id`, `requisite_id`, `prerequisite_course_id`

##### Rename

- Rename `prerequisite_id` to `uid`

##### Partition 

- Generate `partition_uid` by extracting the last characters from `prerequisite_course_id` and `requisite_id` 
  and concatenating them with `-`
- Generate partition matrix by using the wrap around technique and partition the dataframe

##### Ingest

- For each partition, create `PREREQUISITE` relationships from `Course` nodes to `Requisite` nodes 
  by matching on the `uid` column


#### Postrequisites:

##### Load

- Load the prerequisite data (output from the validator) with the following columns:
  `postrequisite_id`, `requisite_id`, `course_id`

##### Rename

- Rename `postrequisite_id` to `uid`

##### Partition 

- Generate `partition_uid` by extracting the last characters from `course_id` and `requisite_id` 
  and concatenating them with `-`
- Generate partition matrix by using the wrap around technique and partition the dataframe

##### Ingest

- For each partition, create `postrequisite_id` relationships from `Course` nodes to `Requisite` nodes 
  by matching on the `uid` column

#### Teaches:

- Load the teaches data (output from the validator) with the following columns:
  `teaches_id`, `course_id`, `professor_id`

##### Rename

- Rename `teaches_id` to `uid`

##### Partition 

- Generate `partition_uid` by extracting the last characters from `professor_id` and `course_id` 
  and concatenating them with `-`
- Generate partition matrix by using the wrap around technique and partition the dataframe

##### Ingest

- For each partition, create `teaches_id` relationships from `Professor` nodes to `Course` nodes 
  by matching on the `uid` column

## Requirements

- Python 3.9 or later

## Environment Variables

Before running the ingestor, make sure to set the following environment variables:

- `FILE_STORAGE_TYPE`: the type of storage to use (either `LOCAL` or `MINIO`)
- `NUMBER_OF_PARTITIONS`: how many partitions should be created for the ingestion of relationships

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

- `STUDY_PROGRAMS_DATA_INPUT_FILE_NAME`: the name of the study_programs input file
- `COURSE_DATA_INPUT_FILE_NAME`: the name of the courses input file
- `PROFESSORS_DATA_INPUT_FILE_NAME`: the name of the professors input file
- `CURRICULA_DATA_INPUT_FILE_NAME`: the name of the curricula input file
- `REQUISITES_DATA_INPUT_FILE_NAME`: the name of the requisites input file
- `OFFERS_DATA_INPUT_FILE_NAME`: the name of the offers input file
- `INCLUDES_DATA_INPUT_FILE_NAME`: the name of the includes input file
- `PREREQUISITES_DATA_INPUT_FILE_NAME`: the name of the prerequisites input file
- `POSTREQUISITES_DATA_INPUT_FILE_NAME`: the name of the postrequisites input file
- `TEACHES_DATA_INPUT_FILE_NAME`: the name of the teaches input file


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
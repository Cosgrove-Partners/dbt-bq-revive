# MWR dbt Project

## Repository Purpose

This repository was created by Cosgrove Partners to store and share scripts and datasets related to our data analysis projects. It contains various files that can be used to perform analyses, generate reports, and automate data-related processes.

## Installation

To use the Python code from this repository, follow the steps below:

1. Clone this repository to your local environment:

    ```bash
      git clone https://github.com/Cosgrove-Partners/dbt-bq-mwr.git
      cd dbt-bq-mwr
    ```

2. Create a virtual environment (recommended):

    ```bash
      python -m venv venv
    ```

3. Activate the virtual environment:

    - On Windows:

    ```bash
      venv\Scripts\activate
    ```

    - On macOS/Linux:

    ```bash
      source venv/bin/activate
    ```

4. Install the necessary dependencies:

    ```bash
      pip install -r requirements.txt
    ```

5. Run the precommit install
     
    ```bash
      pre-commit install
    ```

6. Create the necessary environment variables:

    - Create a `.env` file in the root directory of the repository.
    - Add the following environment variables to the `.env` file:

        ```bash
            DBT_PROFILES_DIR=./
            DBT_TARGET=dev
            DBT_GCP_PROJECT=<project id>
            DBT_GCP_DATASET=<project dataset>
            DBT_GCP_LOCATION=<project location>
            DBT_CRED_TYPE=<credential type>
            DBT_CRED_PKID=<private key id>
            DBT_CRED_CLID=<credential id>
            DBT_CRED_CLEM=<credential email value>
            DBT_CRED_PKEY=<private key value>
        ```

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices

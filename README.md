
# Soccer leagues data to Snowflake

this project was developed an ETL process (using Python, pandas) orchested by Airflow and loaded in Snowflake




## Deployment

To install the astronomer Cli

```bash
  curl -sSL install.astronomer.io | sudo bash -s 
```
to run the docker container with airflow

```bash
  astro dev start
```
It is also necessary to have a Snowflake account, create the datawarehouse, the database and the table for data upload.  

In the Airflow user interface, create the connection between Snowflake and Airflow (if the name is different from "airflow_snowflake", change the name in the files where "snowflake_conn_id='airflow_snowflake'"). Also, set the environment variable "feature_info" with the information from the setup_env_var_ariflow.txt file.

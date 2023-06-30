# Weather_API-ETL-Pipeline
This project aims to automate the collection and processing of real-time weather data using Airflow, an open-source platform for workflow management. The pipeline orchestrates the entire workflow, from data ingestion to storage in a data warehouse and sending text notification to user's phone.
## Features
* Integration with OpenWeatherMap API for retrieving real-time weather data.
* Automated data transformation tasks to convert raw JSON weather data into a structured format.
* Utilization of Pandas library for efficient data manipulation and transformation operations.
* Storage of transformed data in CSV format.
* Configuration of AWS S3 to securely store the weather data.
* Automated Notifications using AWS Lambda to trigger SNS which sends weather information to the user's mobile device.
* Loading of transformed data into an AWS Redshift data warehouse for further analysis.
Implementation of error handling mechanisms and logging to ensure pipeline reliability.
* Scheduled execution of pipeline tasks for regular updates and data refresh.
Data quality checks to validate the accuracy and integrity of the transformed data.
## Installation
*  Clone the repository:
```python 
git clone https://github.com/mastanvali2724/Weather_API-ETL-Pipeline.git
```
* Install the required dependencies:
```python 
pip install -r requirements.txt
```
* Set up the necessary configuration parameters in .env file.
## Usage
* Ensure that Airflow is up and running, and the necessary connections and variables are configured.
* Copy the DAG file Data_pipeline.py to the DAGs directory of your Airflow installation.
* Trigger the DAG in the Airflow UI or use the Airflow CLI to start the pipeline execution.
* The pipeline will automatically fetch real-time weather data, transform it, and load it into the data warehouse.
* Setup the Lambda Code with necessary roles and create topic in SNS and verify the user's mobile endpoint.
* Monitor the Airflow UI or logs for any errors or issues during the pipeline execution.

Contributing
Contributions are welcome! If you find any bugs or have suggestions for improvement, please open an issue or submit a pull request.

License
MIT License

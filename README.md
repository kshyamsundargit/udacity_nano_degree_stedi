# Data Engineering nano-degree - STEDI App Data Analysis

As a data engineer on the STEDI Step Trainer team,  need to extract the data produced by the STEDI Step Trainer sensors and the mobile app, and curate them into a data lakehouse solution on AWS so that Data Scientists can train the learning model.



### Project Data:
STEDI has three JSON data sources(opens in a new tab) to use from the Step Trainer. Check out the JSON data in the following folders in the Github repo:

- customer
- step_trainer
- accelerometer


### Project Workflow:

Using AWS Glue, AWS S3, Python, and Spark, create or generate Python scripts to build a lakehouse solution in AWS that satisfies these requirements from the STEDI data scientists

![image](https://github.com/user-attachments/assets/47e2ab38-13b6-4413-a107-a76dfd42c975)



### Project Steps:
AWS role was created and provided needed permission,  so that Glue can connect to S3 buckets.

Above mentioned project work flow considered and Glue jobs were created accordingly.
1. Landing jobs : Files downloaded from Udacity website are loaded into corresponding S3 bucket folders
   - customer_landing
   - accelerometer_landing
   - ste_trainer
2. Trusted jobs :  To clean the data as per the requirements
   - cutomer_trusted  :  Sanitize the Customer data from the Website (Landing Zone) and only store the Customer Records who agreed to share their data for research purposes (Trusted Zone)
   - accelerometer_trusted  : Sanitize the Accelerometer data from the Mobile App (Landing Zone) - and only store Accelerometer Readings from customers who agreed to share their data for research purposes (Trusted Zone)
   - step_trainer_trusted :  Step Trainer Records data for customers who have accelerometer data and have agreed to share their data for research
3. Curated jobs :  to create final output based on machinelearning requirements
   - customer_curated :  Sanitize the Customer data (Trusted Zone) and create a Glue Table (Curated Zone) that only includes customers who have accelerometer data and have agreed to share their data for research
   - machine_learnign_curated :  aggregated table that has each of the Step Trainer Readings, and the associated accelerometer reading data for the same timestamp, but only for customers who have agreed to share their data
4. Glue tables were created for all the outputs,  so that can query the data in AWS Athena. DDL queries are included in the sql_code folder

![image](https://github.com/user-attachments/assets/ed0960b3-db3e-4460-9a06-84bfacb40ce3)

5. After each stage of your project, table row_count and records were checked and they matched with the expected volume.  screen shots are included in the screen_shots folder to show the results of Athena queries.
  
Landing
   Customer: 956
   Accelerometer: 81273
   Step Trainer: 28680
Trusted
   Customer: 482
   Accelerometer: 40981
   Step Trainer: 14460
Curated
   Customer: 482
   Machine Learning: 43681








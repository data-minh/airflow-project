# airflow-project

Process data with data from sources including Questions.csv and Answers.csv:
https://drive.google.com/drive/folders/1uq4TNKlSE-a_UuVSUSidartcUtRGmS30

Process data with spark, use docker compose to build the project:
airflow:2.8.2
mongodb:4.4
spark:3.4.1
JDK:17
Docker

Processing content is to count the number of questions to see how many answers there are.

GUIDE:

Step 1: Create a folder: airflow

Step 2: Move all files to the airflow folder

Step 3: Run bash: docker compose up

Step 4: Run bash: docker compose down

Step 5: Run bash: docker build -t my_airflow .

Step 6: Renamed image: my_airflow ở file docker-compose.yaml

Step 7: docker compose up

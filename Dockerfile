FROM apache/airflow:2.8.2
# setup things
USER root
# setup pkgs
RUN apt-get update && apt-get install -y wget gnupg

RUN mkdir /content
# mongosh
RUN wget -qO "/content/mongodb-mongosh_amd64.deb" "https://downloads.mongodb.com/compass/mongodb-mongosh_1.9.1_amd64.deb"
RUN dpkg -i "/content/mongodb-mongosh_amd64.deb"

# mongodb-database-tools
RUN wget -qO "/content/mongodb-database-tools.deb" "https://fastdl.mongodb.org/tools/db/mongodb-database-tools-debian11-x86_64-100.7.1.deb"
RUN dpkg -i "/content/mongodb-database-tools.deb"
# setup things
USER airflow
RUN pip install -U apache-airflow-providers-apache-spark pyspark findspark --user

# REMEMBER TO BUILD ME
# docker build -t my_airflow .

# Cài đặt OpenJDK 17
USER root
RUN apt-get install -y openjdk-17-jdk

# Thiết lập biến môi trường JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64



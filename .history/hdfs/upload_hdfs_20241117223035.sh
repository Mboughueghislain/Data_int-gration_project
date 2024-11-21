#!/bin/bash
hdfs dfs -mkdir -p /hospital_data
hdfs dfs -put /home/ghislain/Efrei/Data_Integeration_kafka/data/in-hospital-mortality-trends-by-diagnosis-type.csv /hospital_data/
hdfs dfs -put /home/ghislain/Efrei/Data_Integeration/data/in-hospital-mortality-trends-by-health-category.csv /hospital_data/

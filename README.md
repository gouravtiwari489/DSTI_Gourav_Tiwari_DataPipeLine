# DSTI_PROJECT_DATA_PIPELINE



## Generate result for customer list 
1) Clone Repository using
2) Alternatively running spark command "spark-submit --master local[*] --class main.java.OlistDataProcessing target\Olist-1.0-SNAPSHOT.jar"
2) The output will be in customerdata folder 


## Process of running spark job on AWS

1) Log in to AWS account
2) Create S3 bucket
3) Upload JAR file and resource folder on bucket using AWS CLI/Console
4) Goto AWS EMR console and create cluster by providing s3 bucket folder and launch mode as Step execution
5) Provide type as "Spark Application" and steps or commands
r 

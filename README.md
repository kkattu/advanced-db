# advanced-db
LA crime big data analysis

Steps:

Install Spark and HDFS in your system.
Download crime and other data
Import the data into HDFS with  hadoop fs -put /local-file-path /user/user/input/
Run q0.py from this directory with spark-submit q0.py
Run any other script using spark-submit {script_name}.py
Output files are stored in hdfs. Use hadoop fs -get /hdfs-file-path  /local-file-path to copy output file to local filesystem

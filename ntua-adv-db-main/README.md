# ntua-adv-db

## Steps:

1. Install Spark and HDFS in your system.
2. Download crime and other data
3. Import the data into HDFS with  ```hadoop fs -put /local-file-path /user/user/input/```
4. Run q0.py from this directory with ```spark-submit q0.py```
5. Run any other script using ```spark-submit {script_name}.py```

Output files are stored in hdfs. Use ``` hadoop fs -get /hdfs-file-path  /local-file-path ``` to copy output file to local filesystem.
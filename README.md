# SETUP
1) Install Java 17
    - In a C drive create a new Folder called jdk17 and put all the dowloaded files
2) Install Spark
    - In a C drive create a new Folder called spark and put all the dowloaded files
3) Install WinUtils
    - In a C drive create a new Folder called hadoop/bin and put there wintuils.exe from correct hadoop version folder/bin
4) Search for an "Edit the system environment variables' and add:
    - JAVA_HOME -> \jdk-17
    - SPARK_HOME-> \spark
    - HADOOP_HOME -> \hadoop
    - SPARK_LOCAL_HOSTNAME -> localhost
    To corensponding folders that we have created in points 1-3
5) For Python' system environment ('Path') edit it by adding there:
    - %JAVA_HOME%\bin 
    - %SPARK_HOME%\bin
    - %HADOOP_HOME5\bin
6) Check if everything is configured correctyly by going to cmd and doing commands:
    - java -version
    - shell-spark
    - pyspark

You can follow:
https://www.youtube.com/watch?v=49yQ-bdj4Ww

# DataComparator
## Description
It is a class based on pyspark written in Python. You can use it to check the differences between two DataFrames:
 - Columns
 - Row counts
 - Values
## How to use it
Import the class or copy the code to your notebook and create an object with this tool
`from DataComparator import DataComparator`
`comparator = DataComparator('legacy_data.csv', cloud_df, ['id'], spark)`
As you can see this tool is taking 4 arguments:
1) df1 variable:
    * .csv - `data.csv`
    * pandas dataframe - `<DataFrame variable>`
    * spark dataframe - `<DataFrame variable>`
2) df2 variable:
    * .csv - `data.csv`
    * pandas dataframe - `<DataFrame variable>`
    * spark dataframe - `<DataFrame variable>`
3) primary_key variable - `['id']`
4) spark session variable - `spark`

Then you can call .summarize() function with optional argument True which, if on, will create .txt file with a result (log)
```python
report = comparator.summarize(True)
report
```

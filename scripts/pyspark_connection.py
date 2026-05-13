import os
import sys
from pyspark.sql import SparkSession

def create_spark_sessions():
    # 1. The Core Paths
    os.environ['PATH'] = os.environ['PATH'] + ";" + r"D:\hadoop\bin"
    os.environ['JAVA_HOME'] = r"D:\Java\jdk-17\jdk-17.0.19+10"
    os.environ['HADOOP_HOME'] = r"D:\hadoop"
    
    # 2. THE MISSING LINK: Tell Java exactly where the PySpark JARs live in your venv
    os.environ['SPARK_HOME'] = r"D:\Dmart_PySpark_Project\venv\Lib\site-packages\pyspark"
    
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    
    # 3. Wipe this variable in case it's conflicting with the local environment
    if 'PYSPARK_SUBMIT_ARGS' in os.environ:
        del os.environ['PYSPARK_SUBMIT_ARGS']
        
    # 4. Bind the PATH
    os.environ['PATH'] = os.environ['JAVA_HOME'] + r"\bin;" + os.environ['HADOOP_HOME'] + r"\bin;" + os.environ['PATH']

    # 5. Build the Session
    return SparkSession.builder \
        .appName("DmartProject") \
        .master("local[*]") \
        .getOrCreate()
# # In this notebook, we'll examine the results of our NLP engine.
# As the EmberSession API for Python is not finalized, we'll have to work directly with the processed data.
# 
# First, let's read in our processed data into a DataFrame.

# Importing Spark.
from pyspark.sql import SparkSession
from pyspark.files import SparkFiles
from pyspark.sql.functions import *


# Instantiating the SparkSession.
spark = SparkSession.builder.getOrCreate()

# Reading in the DataFrame.
df = spark.read.parquet("hdfs:///FILE-PATH")

# Let's take a look at our Spark DataFrame's schema.
df.printSchema()

# # Analyzing the NLP Results
# Assuming everything ran correctly, you the schema above should match that of the original data source,
# plus one additional column with the below schema.

print("""
|-- emberNLP: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- text: string (nullable = true)
 |    |    |-- subject: string (nullable = true)
 |    |    |-- polarity: integer (nullable = true)
 |    |    |-- confidence: float (nullable = true)
 |    |    |-- historyOf: integer (nullable = true)
 |    |    |-- textBegin: integer (nullable = true)
 |    |    |-- textEnd: integer (nullable = true)
 |    |    |-- ontologyMappings: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- preferredText: string (nullable = true)
 |    |    |    |    |-- codingScheme: string (nullable = true)
 |    |    |    |    |-- code: string (nullable = true)
 |    |    |    |    |-- cui: string (nullable = true)
 |    |    |    |    |-- tui: string (nullable = true)
 """)

# This column contains the results of Ember's NLP Analysis.

# We can filter on the clinical text in each DataFrame.
# For example, we can check how many of these documents contain the word cancer.

print df.count()
print df.filter(array_contains(col("emberNLP.text"), "cancer")).count()

# As we have a complex annotation structure of arrays of structs containing arrays of structs,
# it helps to explode the array columns to make engineering the data more tractable.
exploded_df = df.select(col("KEY_COLUMN"), explode("emberNLP").alias("emberNLP"))
exploded_df.printSchema()

# Finally, let's mirror the logic of our Ember's Code Search and look for positive instances of the
# snomed code for cancer.

print (exploded_df
       .filter(array_contains(col("emberNLP.ontologyMappings.code"),"363346000") & (col("emberNLP.polarity") == 1))
       .select("KEY_COLUMN")
       .drop_duplicates()
       .count())
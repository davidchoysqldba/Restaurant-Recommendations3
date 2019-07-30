# data_path = '/home/developer/dev-labs/python-dev/data/data2'
# data_path = 'gs://rest_pipeline_2018/data/*.json'
# data_path = 'gs://rest_pipeline_2018/data/in_gcp'

data_path = ""
data_path_bq = ""



def PassInPathAndRun():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("-data_path", required=True, dest="data_path", help="Specify data_path.")
    parser.add_argument("-data_path_bq", required=True, dest="data_path_bq", help="Specify data_path_bq.")
    args = parser.parse_args()

    return args.data_path, args.data_path_bq

if __name__ == '__main__':
    data_path, data_path_bq = PassInPathAndRun()

# if data_path == None or data_path_bq == None:
#     sc.stop()


print("path: ", data_path, data_path_bq)

from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[4]").appName("Load Star Schema").getOrCreate()
sc = spark.sparkContext


# Import all libraries
import json
import numpy as np
import functools

from pyspark.sql import Row
from pyspark.sql import Window
from pyspark.sql.functions import rank, col, desc, asc
from pyspark.sql import functions as F
from pyspark.sql import types as t

from difflib import SequenceMatcher


### section 1: fills out missing columns ##
##########################################################################################

# a. Define mapping functions
#############################
# Define map function for reading first record of json file with column names
def flat_map_first_row(rec):
    rows = []
    json_obj = json.loads((rec))
    for e in json_obj:
        row_dict = {str(k): v for k, v in e.items()}
        rows.append(Row(**row_dict))
    return rows[0]


# Define map function for creating rdd rows with fillin mising column
def flat_map_json_with_fillin_missing_column(rec, column_names):
    rows = []
    json_obj = json.loads((rec))
    for e in json_obj:
        row_dict = {str(k): v for k, v in e.items()}
        for c in column_names:
            if c not in row_dict.keys():
                row_dict[c] = None
        rows.append(Row(**row_dict))
    return rows


# Create rdd to represent first row of all files
def return_rdd_of_first_row():
    return sc.wholeTextFiles(data_path + '/*.json').filter(lambda x: x[1] != "[]\n").map(lambda x: x[1]).map(flat_map_first_row)
# rdd_first_row.take(5)


# Define function to return column name has all column names from all first json records
def return_column_names(rdd_row):
    column_names = []
    for i in rdd_row.toLocalIterator():
        column_names.extend(i.asDict().keys())
    return set(column_names)


# b. Implementing functions
###########################
rdd_first_row = return_rdd_of_first_row()
column_names = return_column_names(rdd_first_row)
# exec time < 4 min

# print("column names: ", column_names)
##########################################################################################



# Section 2: Process all files
##########################################################################################
# a. Specify mapping functions

# Specify reference to json row mapping function with column name passed in's - this will fill any missing columns with NULL
parsing_func = functools.partial(flat_map_json_with_fillin_missing_column, column_names=column_names)


# Read through all files with implement filling in missing column names
def return_rdd_of_all_file():
    return sc.wholeTextFiles(data_path + '/*.json').filter(lambda x: x[1] != "[]\n").map(lambda x: x[1]).flatMap(parsing_func)
# rdd.take(5)


# b. Implementing functions
rdd = return_rdd_of_all_file()

# Create pyspark dataframe from the rdd object (rdd is a row or dictionary format of data)
df = spark.createDataFrame(rdd)

# Cache datafreame - df
df.cache()



#########################################################
# 3 Define business dataframe function
#########################################################
def create_business_dataframe(df):
    df_business = df.select(["cuisine_description", "camis", "dba", "boro", "building", "phone", "street", "zipcode", "inspection_date"]).distinct()#.collect()
    return df_business

def create_business_latest_dataframe(df_business):
    # window = Window.partitionBy("camis").orderBy(desc("inspection_date"))
    # df_business_latest = (df_business.withColumn('rank', rank().over(window)).filter(col('rank') == 1..drop('rank')))
    #
    # df_business_latest = (df_business.withColumn('rank', rank().over(window)),
    # .filter(col('rank') == 1),
    # .drop('rank'),
    # ")\n",

    window = Window.partitionBy("camis").orderBy(desc("inspection_date"))
    df_business_latest = (df_business.withColumn('rank', rank().over(window))
                          .filter(col('rank') == 1)
                          .drop('rank')
                          )

    return df_business_latest

#########################################################
# 4 Define business grade dataframe function
#########################################################

def create_business_grade_dataframe(df):
    df_business_grade_detail = df.where(col('grade').isNotNull()).select(['camis', 'score', 'grade', 'grade_date']).distinct()
    # df_business_grade_detail.take(5)

    # Define function to return grade number from letter

    def grade_letter_dict_to_number(key):
        dict = {'A': 5, 'B': 4, 'C': 3}
        return dict.get(key)
    
    # Define pyspark user define function for mapping grade letter to grade number
    grade_udf = F.udf(grade_letter_dict_to_number, t.IntegerType())

    # Create new grade number column from grade (letter) column using dict udf function
    df_business_grade_detail = df_business_grade_detail.withColumn('grade_number', grade_udf(col('grade')))

    # Exclude business grade d.f. of the following: exclude NULL and grade 'Not Yet Graded', 'G', 'P', 'Z'
    list_grade_exclude = {'Not Yet Graded', 'G', 'P', 'Z'}
    df_business_grade_detail2 = df_business_grade_detail.where(col('grade').isNotNull()).filter(~col('grade').isin(list_grade_exclude))#.select(col('grade')).distinct().collect()
    # df_business_grade.take(5)

    # Create business grade info d.f. of the following:
    # lowest grade, highest grade, average grade, and how many given grades
    df_business_grade = df_business_grade_detail2.groupBy(col('camis')).agg(F.min('grade_number').alias('grade_lowest'), F.max('grade_number').alias('grade_highest'), F.mean('grade_number').alias('grade_average'), F.count('grade_number').alias('grade_count')) #\
    # df_business_grade.take(5)
    # # < 1 mins

    return df_business_grade


##############################################################################
# 5 Define business restaurant type and similarity measures dataframe function
##############################################################################


def similarity_score(a, b):
    return SequenceMatcher(None, a, b).ratio() * 100


def create_business_cuisine_similarity_join(df_business_latest):

    udf_similarity_score = F.udf(similarity_score, t.DoubleType())

    df_business_cuisine = df_business_latest.select(['camis', 'cuisine_description', 'zipcode']).distinct()

    df_bc1 = df_business_cuisine.alias('df_bc1')

    df_bc2 = df_business_cuisine.alias('df_bc2')

    df_bc_join = df_bc1.join(df_bc2, (df_bc1['zipcode'] == df_bc2['zipcode']) & (df_bc1['camis'] != df_bc2['camis']) )

    df_bc_joined = df_bc_join.select(["df_bc1.camis", "df_bc1.cuisine_description", "df_bc2.camis", "df_bc2.cuisine_description"]).withColumn("matching_camis", col("df_bc2.camis")).withColumn("matching_cuisine_description", col("df_bc2.cuisine_description")).drop(col("df_bc2.camis")).drop(col("df_bc2.cuisine_description"))

    df_bc_compared_set = df_bc_joined.withColumn("similarity_score", udf_similarity_score(col("cuisine_description"), col("matching_cuisine_description")))

    # df_bc_compared_set.take(20)
    df_bc_compared_set.cache()

    df_bc_compared_similar_list = df_bc_compared_set.filter(col("similarity_score") >= 80).orderBy(asc("similarity_score"))
    # df_bc_compared_similar_list.take(10)
    return df_bc_compared_similar_list

##############################################################################
# 6 Run and create all dataframes
##############################################################################

df_business = create_business_dataframe(df)
# print(df_business.show(1))

df_business_grade = create_business_grade_dataframe(df)
# print(df_business_grade.show(1))

df_business_latest = create_business_latest_dataframe((df_business))
# print(df_business_latest.show(1))

df_bc_compared_similar_list = create_business_cuisine_similarity_join(df_business_latest)

##############################################################################
# 6 Run and create all dataframes
##############################################################################

df_business_file = df_business.coalesce(1)
df_business_file.write.csv(data_path_bq + "/business.csv", mode="overwrite", header="True")

df_business_grade_file = df_business_grade.coalesce(1)
df_business_grade_file.write.csv(data_path_bq + "/df_business_grade.csv", mode="overwrite", header="True")

df_bc_compared_similar_list_file = df_bc_compared_similar_list.coalesce(1)
df_bc_compared_similar_list_file.write.csv(data_path_bq + "/df_bc_compared_similar_list.csv", mode="overwrite", header="True")


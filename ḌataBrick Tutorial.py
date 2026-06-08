# Databricks notebook source
# MAGIC %md
# MAGIC ###Data Reading JSON

# COMMAND ----------

df_json = spark.read.table('default.drivers')

# COMMAND ----------

# DBTITLE 1,Cell 3
df_json.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Data Reading CSV
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cell 2
# Read from the existing big_mart_sales table instead of the DBFS path
df = spark.table('default.big_mart_sales')



# COMMAND ----------

df.show()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Print Schema

# COMMAND ----------

df.printSchema()

# COMMAND ----------

## Convert Item name to String 
my_ddlSchema='''
                Item_Identifier String,
                Item_Weight String,
                Item_Fat_Content String,
                Item_Visibility Double,
                Item_Type String,
                Item_MRP Double,
                Outlet_Identifier String,
                Outlet_Establishment_Year Long,
                Outlet_Size String,
                Outlet_Location_Type String,
                Outlet_Type String,
                Item_Outlet_Sales Double
                '''


# COMMAND ----------

# DBTITLE 1,Cell 11
# Apply the schema from my_ddlSchema using StructType.fromDDL()
from pyspark.sql.types import StructType

# Parse the DDL schema string
target_schema = StructType.fromDDL(my_ddlSchema)

# Read the table and apply the schema by casting each column
df = spark.table('default.big_mart_sales')
for field in target_schema.fields:
    df = df.withColumn(field.name, df[field.name].cast(field.dataType))

df.show()
df.display()
df.printSchema()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Structype Schema()

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

my_strct_schema=StructType([
                                StructField('Item_Identfier',StringType(),True),
                                StructField('Item_Weight',StringType(),True),
                                StructField('Item_Fat_Content',StringType(),True),
                                StructField('Item_Visibility',DoubleType(),True),
                                StructField('Item_Type',StringType(),True),
                                StructField('Item_MRP',DoubleType(),True),
                                StructField('Outlet_Identifier',StringType(),True),
                                StructField('Outlet_Establishment_Year',IntegerType(),True),
                                StructField('Outlet_Size',StringType(),True),
                                StructField('Outlet_Location_Type',StringType(),True),
                                StructField('Outlet_Type',StringType(),True),
                                StructField('Item_Outlet_Sales',DoubleType(),True)

])

# COMMAND ----------

# DBTITLE 1,Cell 16
# Reload df from table to ensure it's properly initialized
df = spark.table('default.big_mart_sales')
df.printSchema()

# COMMAND ----------

df = spark.table('default.big_mart_sales')

# COMMAND ----------

# MAGIC %md
# MAGIC ###SELECT

# COMMAND ----------

df.display()

# COMMAND ----------

df_sel=df.select('Item_Identifier','Item_Weight','Item_Identifier').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ALIAS

# COMMAND ----------

# DBTITLE 1,Cell 23
from pyspark.sql.functions import col
df.select(col('Item_Identifier').alias('ItemId')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###FILTER/WHERE

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenerio 1

# COMMAND ----------

df.display()

# COMMAND ----------

df.filter(col('Item_Fat_Content')=='Regular').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenerio 2

# COMMAND ----------

df.filter((col('Item_Type') =='Soft Drinks') & (col('Item_Weight')<10)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenerio 3

# COMMAND ----------

df.filter((col('Outlet_Size').isNull()) & (col('Outlet_Location_Type').isin(['Tier 1','Tier 2']))).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### withcolumnrename()

# COMMAND ----------

df.withColumnRenamed('Item_Weight','Weight').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### withColumn

# COMMAND ----------

# MAGIC %md
# MAGIC new_col

# COMMAND ----------

df=df.withColumn('flag',lit("new")).display()

# COMMAND ----------

# DBTITLE 1,Cell 37
df = spark.table('default.big_mart_sales')
df.withColumn('multiply',col('Item_Weight')*col('Item_MRP')).display()

# COMMAND ----------

df.withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),'LF','Low Fat'))\
    .withColumn('Item_fat_Content',regexp_replace(col('Item_Fat_Content'),'Regular','Regular Fat')).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Type Casting

# COMMAND ----------

df=df.withColumn('Item_Weight',col('Item_Weight').cast(StringType()))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Sort/Order By
# MAGIC

# COMMAND ----------

df.sort(col('Item_Weight').desc()).display()

# COMMAND ----------

df.sort(col('Item_Visibility').asc()).display()

# COMMAND ----------

# DBTITLE 1,Cell 45
df.sort('Item_Weight','Item_Visibility',ascending=[0,0]).display()

# COMMAND ----------

df.sort(['Item_Weight','Item_Visibility'],acsending=[1,0]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###LIMIT

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DROP

# COMMAND ----------

df.drop('Item_Visibility').display()

# COMMAND ----------

df.drop('Item_Visibility','Item_Weight').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DROP DUPLICATES

# COMMAND ----------

df.dropDuplicates().display()

# COMMAND ----------

df.dropDuplicates(subset=['Item_Type']).display()

# COMMAND ----------

df.distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Union and Union Byname

# COMMAND ----------

# MAGIC %md
# MAGIC #### Preparing Data Frames
# MAGIC

# COMMAND ----------

data_1 = [(1,'kad'),(2,'sid')]
schema1 = 'id String, name String'

df1=spark.createDataFrame(data_1,schema1)

data_2 = [(3,'rahul'),(4,'sid')]
schema2 = 'id String, name String'

df2=spark.createDataFrame(data_2,schema2)


# COMMAND ----------

# DBTITLE 1,Cell 59
df1.display()

# COMMAND ----------

df2.display()

# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------

data_1 = [('kad',1),('sid',2)]
schema1 = 'name String, id String'

df1=spark.createDataFrame(data_1,schema1)
df1.union(df2).display()

# COMMAND ----------

df1.unionByName(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Init cap, lower case, upper case

# COMMAND ----------

# DBTITLE 1,Cell 65
from pyspark.sql.functions import *
df.select(initcap('Item_Type')).display()

# COMMAND ----------

df.select(lower('Item_Type')).display()

# COMMAND ----------

df.select(upper('Item_Type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### CurrentDate, Date add, Date sub

# COMMAND ----------

df=df.withColumn('curr_date',current_date())
df.display()

# COMMAND ----------

df=df.withColumn('week after',date_add('curr_date',7)).display()

# COMMAND ----------

# DBTITLE 1,Cell 71
df = spark.table('default.big_mart_sales')
df = df.withColumn('curr_date', current_date())
df=df.withColumn('week before',date_sub('curr_date',7))
df.display()
#df.withColumn('week before',date_add('curr_date',-7)).display()

# COMMAND ----------

# DBTITLE 1,Cell 72
df=df.withColumn('datediff',datediff('curr_date','week before'))
df.display()

# COMMAND ----------

# DBTITLE 1,Cell 73
# Reload df to clear any cached transformations from previous cell
df = spark.table('default.big_mart_sales')
df = df.withColumn('curr_date', current_date())
df = df.withColumn('week before', date_sub('curr_date', 7))
df = df.withColumn('datediff', datediff('curr_date', 'week before'))
df = df.withColumn('week before', date_format('week before', 'dd-MM-yyyy'))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Handling NULL VALUES

# COMMAND ----------

df.dropna('all').display()

# COMMAND ----------

df.dropna('any').display()

# COMMAND ----------

df.dropna(subset=['Outlet_Size']).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Filling Nulls
# MAGIC

# COMMAND ----------

df.fillna('Not_Availale').display()

# COMMAND ----------

df.fillna('Not_Avaialble',subset=['Outlet_Size']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Split and indexing

# COMMAND ----------

df.withColumn('Outlet_Type',split('Outlet_Type',' ')).display()

# COMMAND ----------

df.withColumn('Outlet_Type',split('Outlet_Type',' ')[1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explode

# COMMAND ----------

df_extra=df.withColumn('Outlet_Type',split('Outlet_type',' '))
df_extra.display()

# COMMAND ----------

df_extra.withColumn('Outlet_Type',explode('Outlet_Type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Array Contains

# COMMAND ----------

df_extra.withColumn('Type1Flag',array_contains('Outlet_Type','Type1')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Group By

# COMMAND ----------

df.groupBy('Item_Type').agg(sum("Item_MRP")).display()

# COMMAND ----------

df.groupBy('Item_Type','Outlet_Size').agg(sum('Item_MRP').alias("Totol MRP")).display()

# COMMAND ----------

df = spark.table('default.big_mart_sales')

# COMMAND ----------

# DBTITLE 1,Cell 92
from pyspark.sql.functions import *
df.groupBy('Item_Type','Outlet_Size').agg(sum('Item_MRP'),avg('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Collect_List

# COMMAND ----------

df.groupBy('Item_Type').agg(collect_list('Outlet_Establishment_Year')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pivot

# COMMAND ----------

df.groupBy('Item_Type').pivot('Outlet_Size').agg(avg('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### When Otherwise

# COMMAND ----------

df.withColumn('Veg_Flag',when(col("Item_Type")=='Meat','Non-Veg').otherwise('Veg')).display()

# COMMAND ----------

# DBTITLE 1,Cell 99
df.withColumn('Veg_Exp_Flag',when((col("Item_Type") != 'Meat') & (col("Item_MRP")<100),"Veg_Inexpensive")\
    .when((col("Item_Type") != 'Meat') & (col("Item_MRP")>100),"Veg_Expensive").otherwise('NA')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Joins

# COMMAND ----------

dataj1 = [('1', 'gaur', 'd01'),
        ('2', 'kit', 'd02'),
        ('3','sam', 'd03'),
        ('4', 'tim', 'd03'),
        ('5','aman', 'd05')]

schemaj1='emp_id STRING, emp_name STRING, dept_id STRING'

df1= spark.createDataFrame(dataj1, schemaj1)

dataj2 = [('d01', 'HR'),
        ('d02', 'Marketing'),
        ('d03', 'Accounts'),
        ('d04', 'IT'),
        ('d05', 'Finance')]

schemaj2= 'dept_id STRING, department STRING'

df2= spark.createDataFrame (dataj2, schemaj2)

# COMMAND ----------

df1.display()

# COMMAND ----------

df2.display()

# COMMAND ----------

df1.join(df2,df1['dept_id']==df2['dept_id'],'inner').display()

# COMMAND ----------

df1.join(df2,df1['dept_id']==df2['dept_id'],'left').display()

# COMMAND ----------

df1.join(df2,df1['dept_id']==df2['dept_id'],'right').display()

# COMMAND ----------

df1.join(df2,df1['dept_id']==df2['dept_id'],'anti').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widnow Functions

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

from pyspark.sql.functions import row_number
df.withColumn('rowCol',row_number().over(Window.orderBy('Item_Identifier'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rank and Dense Rank

# COMMAND ----------

from pyspark.sql.functions import *
df.withColumn('rank', rank().over(Window.partitionBy(col('Item_Identifier')).orderBy(col('Item_Outlet_Sales')))).display()

# COMMAND ----------

rank_window = Window.partitionBy("Item_Identifier").orderBy("Item_Outlet_Sales")

dense_rank_window = Window.orderBy(desc("Item_Outlet_Sales"))

df = df.withColumn(
    "rank",
    rank().over(rank_window)
).withColumn(
    "dense_rank",
    dense_rank().over(dense_rank_window)
)

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cumulative Sum

# COMMAND ----------

df.withColumn('cumsum',sum('Item_MRP').over(Window.orderBy('Item_Type'))).display()

# COMMAND ----------

df = spark.table('default.big_mart_sales')

# COMMAND ----------

df.withColumn('cumsum',sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding,Window.currentRow))).display()

# COMMAND ----------

df.withColumn('cumsum',sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### User Defined Functions (UDF)

# COMMAND ----------

def my_fun(x):
    return x*x
print(my_fun(3))

# COMMAND ----------

my_udf=udf(my_fun)

# COMMAND ----------

df.withColumn('mynewcol',my_udf(col('Item_MRP'))).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Writing

# COMMAND ----------

df.write.saveAsTable('workspace.default.data_csv')

# COMMAND ----------

# MAGIC  %md
# MAGIC  #### Modes- Append, Overwrite, Error, Ignore

# COMMAND ----------

df.write.mode('append').saveAsTable('workspace.default.data_csv')

# COMMAND ----------

df.write.mode('overwrite').saveAsTable('workspace.default.data_csv')

# COMMAND ----------

df.write.mode('error').saveAsTable('workspace.default.data_csv')

# COMMAND ----------

df.write.mode('ignore').saveAsTable('workspace.default.data_csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parquet File Format- It is stored as row based file format and it is very handy when we wish to write data means in OLTP databases or transactional databases but in case of big data we cannot use this file format as it used as columnar. Metadata is stored at the footer of the file

# COMMAND ----------

spark.sql("SHOW CATALOGS").show(truncate=False)

# COMMAND ----------

spark.sql("SHOW SCHEMAS").show(truncate=False)

# COMMAND ----------

df.write.mode("overwrite") \
    .parquet("/Volumes/workspace/default/myvolume/data_csv")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Data File Format - built over parquet file. Metadata is stored in seperate file (delta log) not in the same file

# COMMAND ----------

# MAGIC %md
# MAGIC ###Managed vs External Tables
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC SPARK SQL- createTempView

# COMMAND ----------

df.createTempView('my_view')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from my_view

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from my_view where Item_fat_Content='LF'

# COMMAND ----------

df_new=spark.sql("select * from my_view where Item_fat_Content='LF'")
df_new.display()

# COMMAND ----------



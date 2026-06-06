# Databricks notebook source
df=spark.read.format('csv').option("Inferschema",True).option("header",True).load("/Volumes/pyspark/default/pyspark/BigMart Sales.csv")

# COMMAND ----------

df.display(10)

# COMMAND ----------

df_json=spark.read.format('json').option("Inferschema",True).option("header",True).option("multiline",False).load("/Volumes/pyspark/default/pyspark/drivers.json")

# COMMAND ----------

df_json.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------


my_ddl_schema = '''
                    Item_Identifier STRING,
                    Item_Weight STRING,
                    Item_Fat_Content STRING, 
                    Item_Visibility DOUBLE,
                    Item_Type STRING,
                    Item_MRP DOUBLE,
                    Outlet_Identifier STRING,
                    Outlet_Establishment_Year INT,
                    Outlet_Size STRING,
                    Outlet_Location_Type STRING, 
                    Outlet_Type STRING,
                    Item_Outlet_Sales DOUBLE 

                ''' 

# COMMAND ----------

df=spark.read.format('csv').schema(my_ddl_schema).option("header",True).load("/Volumes/pyspark/default/pyspark/BigMart Sales.csv")

# COMMAND ----------

df.display()
df.printSchema()

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

my_struct_schema=StructType([
                                StructField('Item_Identifier',StringType(),True),
                                StructField('Item_Weight',StringType(),True),
                                StructField('Item_Fat_Content',StringType(),True),
                                StructField('Item_Visibility',StringType(),True),
                                StructField('Item_Type',StringType(),True),
                                StructField('Item_MRP',StringType(),True),
                                StructField('Outlet_Identifier',StringType(),True),
                                StructField('Outlet_Establishment_Year',StringType(),True),
                                StructField('Outlet_Size',StringType(),True),
                                StructField('Outlet_Location_Type',StringType(),True),
                                StructField('Outlet_Type',StringType(),True),
                                StructField('Item_Outlet_Sales',StringType(),True),
])

# COMMAND ----------

df=spark.read.format('csv').schema(my_struct_schema).option("header",True).load("/Volumes/pyspark/default/pyspark/BigMart Sales.csv")

# COMMAND ----------

df.display()

# COMMAND ----------

df_select= df.select(col("Item_Fat_Content"),col("Item_Identifier"),col("Item_MRP")).display()

# COMMAND ----------

df_select= df.select(col("Item_Fat_Content").alias("Item_fat"),col("Item_Identifier"),col("Item_MRP")); df_select.display()

# COMMAND ----------

from pyspark.sql.functions import col
df.filter(col("Item_Fat_Content")=='Regular').display()


# COMMAND ----------

df.filter((col('Item_Type') == 'Soft Drinks') & (col('Item_Weight')>10)).display()  

# COMMAND ----------

df.filter(((col("Outlet_Location_Type") == "Tier 1") | (col("Outlet_Location_Type") == "Tier 2")) & col("Outlet_Size").isNull()).display()

## df.filter((col('Outlet_Size').isNull()) & (col('Outlet_Location_Type').isin('Tier 1','Tier 2'))).display()
     

# COMMAND ----------

df.withColumnRenamed("Item_Identifier","Item_id").display()

# COMMAND ----------

df.withColumn('multiply',col('Item_Weight')*col('Item_MRP')).display()

# COMMAND ----------

df.withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'), 'Regular','Reg'))\
    .withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'), 'Low Fat','lf')).display()

# COMMAND ----------


df = df.withColumn('Item_Weight', col('Item_Weight').cast(StringType())) 

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.sort(col('Item_Weight').desc()).display()

# COMMAND ----------

#df.sort(col('Item_Weight').desc(),col('Item_MRP').desc()).display()

df.sort(['Item_Weight','Item_MRP'],ascending=[0,1]).display()

# COMMAND ----------

df.limit(15).display()

#df.show(15)

# COMMAND ----------

df.drop('Item_Weight').display()

# COMMAND ----------

df.drop('Item_Fat_Content','Item_Identifier').display()

# COMMAND ----------

from pyspark.sql.functions import col, count

df.groupBy("Item_Identifier") \
  .count() \
  .filter(col("count") > 1) \
  .display()

# COMMAND ----------

df_dedup = df.dropDuplicates(["Item_Identifier"])

# COMMAND ----------

df_dedup.groupBy("Item_Identifier") \
        .count() \
        .filter(col("count") > 1) \
        .display()

# COMMAND ----------


data1 = [('1','kad'),
        ('2','sid')]
schema1 = 'id STRING, name STRING' 

df1 = spark.createDataFrame(data1,schema1)

data2 = [('3','rahul'),
        ('4','jas')]
schema2 = 'id STRING, name STRING' 

df2 = spark.createDataFrame(data2,schema2)


# COMMAND ----------


df1.union(df2).display()

# COMMAND ----------


data1 = [('kad','1',),
        ('sid','2',)]
schema1 = 'name STRING, id STRING' 

df1 = spark.createDataFrame(data1,schema1)

df1.display()
     

# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------


df1.unionByName(df2).display()
     

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import *; 

df.select(initcap('Item_Type')).alias('Item_Type_inint').display()

df.select(upper('Item_Type')).alias('Item_Type_upper').display()

# COMMAND ----------


df = df.withColumn('curr_date',current_timestamp())

df.display()
     

# COMMAND ----------

from pyspark.sql.functions import date_add, current_timestamp

df=df.withColumn('week_after',date_add(current_timestamp(), 2))

df.display()

# COMMAND ----------

from pyspark.sql.functions import date_sub, current_timestamp

df.withColumn('week_before',date_sub(current_timestamp(), 2)).display()

# COMMAND ----------

df.withColumn(
    "week_after",
    expr("curr_date - INTERVAL 2 DAYS")
).display()

# COMMAND ----------


df.withColumn('datediff',datediff('week_after','curr_date')).display()

# COMMAND ----------


df.withColumn('curr_date',date_format('curr_date',"dd-MM-yyyy'T'HH:mm:ss.SSSXXX")).display()

# COMMAND ----------

df.dropna('all') #drop records having null i all colums

# COMMAND ----------

df.dropna('any').display() #drop records if any column having null

# COMMAND ----------

df.dropna(subset=['Outlet_size']).display()

# COMMAND ----------

df.fillna('NA',"outlet_size").display()

# COMMAND ----------

from pyspark.sql.functions import col, when

df.withColumn(
    "Item_Weight",
    when(col("Item_Weight").isNull(), "NotAvailable")
    .otherwise(col("Item_Weight").cast("string"))
).display()

# COMMAND ----------

df_exp=df.withColumn('Outlet_Type', split('Outlet_Type', ' '))

df_exp.display()

# COMMAND ----------

df.withColumn('Outlet_Type', split('Outlet_Type', ' ')[1]).display()

# COMMAND ----------

df.withColumn("Outlet_Type_1", split("Outlet_Type", " ")[0]) \
       .withColumn("Outlet_Type_2", split("Outlet_Type", " ")[1]).display()

# COMMAND ----------

df.withColumn('outer_type', explode(split('outlet_type',' '))).display()

# COMMAND ----------

df_exp.withColumn('Type1_flag',array_contains('Outlet_Type','Type1')).display()

# COMMAND ----------

df.groupBy("Item_Type").agg(sum("Item_MRP")).display()

# COMMAND ----------

df.groupBy("Item_Type").agg(avg("Item_MRP")).display()

# COMMAND ----------

df.groupBy("Item_Type","Outlet_Size").agg(sum("Item_MRP").alias("Total_MRP")).display()

# COMMAND ----------

df.groupBy('Item_Type','Outlet_Size').agg(sum('Item_MRP').alias("Total_MRP"),avg('Item_MRP').alias("Avg_MRP")).display()

# COMMAND ----------


data = [('user1','book1'),
        ('user1','book2'),
        ('user2','book2'),
        ('user2','book4'),
        ('user3','book1')]

schema = 'user string, book string'

df_book = spark.createDataFrame(data,schema)

df_book.display()

# COMMAND ----------

#from pyspark.sql import functions as F
df_book.groupBy('user').agg(collect_list('book')).display()


# COMMAND ----------

df.groupBy("Item_Type").pivot("Outlet_size").agg(avg("Item_MRP")).display()

# COMMAND ----------

 df.withColumn("Item_Type_Status", when(col("Item_Type") == "Soft Drinks", "Canned").otherwise("Soft_Canned")).display()

 #df = df.withColumn('veg_flag',when(col('Item_Type')=='Meat','Non-Veg').otherwise('Veg'))

# COMMAND ----------

 df_item= df.withColumn("Item_Type_Status", when((col("Item_Type") == "Soft Drinks") | (col("Item_Type") == "Dairy"), "Canned").otherwise("Soft_Canned"))
 
 df_item.display()


# COMMAND ----------

df_item.withColumn('item_exp_flag',when(((col('Item_Type_Status')=='Canned') & (col('Item_MRP')<100)),'Item_Inexpensive')\
                            .when((col('Item_Type_Status')=='Soft_Canned') & (col('Item_MRP')>100),'item_Expensive')\
                            .otherwise('cheap')).display() 
     

# COMMAND ----------

df.count()

# COMMAND ----------


dataj1 = [('1','gaur','d01'),
          ('2','kit','d02'),
          ('3','sam','d03'),
          ('4','tim','d03'),
          ('5','aman','d05'),
          ('6','nad','d06')] 

schemaj1 = 'emp_id STRING, emp_name STRING, dept_id STRING' 

df1 = spark.createDataFrame(dataj1,schemaj1)

dataj2 = [('d01','HR'),
          ('d02','Marketing'),
          ('d03','Accounts'),
          ('d04','IT'),
          ('d05','Finance')]

schemaj2 = 'dept_id STRING, department STRING'

df2 = spark.createDataFrame(dataj2,schemaj2)

# COMMAND ----------

#df1.join(df2, df1.dept_id == df2.dept_id, 'inner').display()

df1.join(df2, "dept_id", "inner").display()

# COMMAND ----------

df1.join(df2,"dept_id","left").display()

# COMMAND ----------

df1.join(df2,"dept_id","right").display()

# COMMAND ----------

df1.join(df2,"dept_id","anti").display()

# COMMAND ----------


df.display()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number



df.withColumn("row",row_number().over(Window.partitionBy("Item_Identifier").orderBy("Item_Identifier"))).filter(col("row") == 1).drop("row").display()

# COMMAND ----------

from pyspark.sql.functions import rank, col, dense_rank
df.withColumn('rank',rank().over(Window.orderBy(col('Item_Identifier').desc())))\
    .withColumn('dense_rank',dense_rank().over(Window.orderBy(col('Item_Identifier').desc()))).display()


# COMMAND ----------


from pyspark.sql.functions import sum
df.withColumn('cum_sum', sum('Item_MRP').over(Window.orderBy('Item_Type'))).display()


# COMMAND ----------

df.withColumn('cumsum',sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding,Window.currentRow))).display()

# COMMAND ----------

df.withColumn('totalsum',sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing))).display()
     

# COMMAND ----------


#overwrite- overwrite the file
#append - append the file
#ignore - ignore the file
#ErrorIfExists - throw an error if the file exists
df.write.format('csv')\
.mode('append')\
.option('path','/Volumes/pyspark/default/pyspark/output/')\
.save()

# COMMAND ----------

df.write.format('parquet')\
.mode('overwrite')\
.option('path','Volumes/pyspark/default/pyspark/output/')\
.save()

# COMMAND ----------


df.write.format('Delta')\
.mode('overwrite')\
.saveAsTable('my_pyspark_table')


df.display()

# COMMAND ----------

df.createTempView('my_view')

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC select * from my_view where Item_Fat_Content = 'Regular'

# COMMAND ----------

df_sql = spark.sql("select * from my_view where Item_Fat_Content = 'Regular'")

df_sql.display()

# COMMAND ----------


"""Entry point for the ETL application

Sample usage:
docker-compose run etl poetry run python main.py \
  --source /opt/data/transaction.csv \
  --database warehouse\
  --table transactions
"""
import argparse
import configparser

from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument('--source', action='store')
parser.add_argument('--database', action='store')
parser.add_argument('--table', action='store')
args = parser.parse_args()


spark: SparkSession = SparkSession.builder.master('spark://spark:7077')\
.config('spark.driver.extraClassPath',"/opt/bitnami/spark/jars/postgresql-42.2.24.jar")\
.config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.2.24.jar")\
  .getOrCreate()#.config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.2.5.jar").getOrCreate()

# TODO: implement the pipeline. Remove the lines below, they are there just to get started.
#  Feel free to use either Spark DSL (PySpark) or Spark SQL syntax.
#  Both examples do the same, group by transactionId and calculate the sum of `unitsSold`.
df = spark.read.csv(args.source, sep='|', header=True, inferSchema=True)
#################################################################
# Spark DSL example
#df.groupBy('transactionId').sum('unitsSold').show()
#################################################################

#################################################################
# Spark SQL example
df.createOrReplaceTempView('transaction')

df_agg = spark.sql("""
select custId,transactionDate
from transaction
group by custId, transactionDate
""")

df_agg.createOrReplaceTempView('agg_transaction')

df_step = spark.sql("""
select custId,transactionDate, transactionDate- LAG(transactionDate) OVER(PARTITION BY custId ORDER BY transactionDate) AS step
from agg_transaction
""")

df_step.createOrReplaceTempView('transaction_step')

df_grp = spark.sql("""
SELECT custId, 
      COUNT(if(step > 1,1,0)) OVER(PARTITION BY custId ORDER BY transactionDate) grp
    FROM transaction_step
""")

df_grp.createOrReplaceTempView('transaction_group')

df_streak = spark.sql("""
select custID,grp,count(1) streak
from transaction_group
group by custID, grp
""")

df_streak.createOrReplaceTempView('customer_streak')

df_max_streak = spark.sql("""
select custID,max(streak) longest_streak
from customer_streak
group by custID
""")



df_cust_product = spark.sql("""
select custID,productSold,sum(unitsSold) totalUnitsSold
from transaction
group by custID, productSold
""")

df_cust_product.createOrReplaceTempView('customer_product')

df_cust_rank_product = spark.sql("""
select custID,productSold,rank() over (PARTITION BY custID ORDER BY totalUnitsSold desc) rnk_fav
from customer_product
""")

df_cust_rank_product.createOrReplaceTempView('customer_rank_product')

df_cust_fav_product = spark.sql("""
select custID,productSold as favourite_product	
from customer_rank_product
where rnk_fav=1
""")



df_out = df_cust_fav_product.join(df_max_streak,df_cust_fav_product.custID==df_cust_fav_product.custID).drop(df_max_streak.custID)

config = configparser.ConfigParser()
config.read("db_properties.ini")
db_prop = config['postgresql']

df_out.write.option('driver', 'org.postgresql.Driver').jdbc(
  url=f'jdbc:postgresql://db:5432/{args.database}',
  table=args.table,
  mode='overwrite',
  properties={"username":db_prop['username'],"password":db_prop['password']})
#################################################################

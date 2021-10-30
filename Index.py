#!/usr/bin/env python
# coding: utf-8

# In[1]:


# import spark liriaries
from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import regexp_extract
from pyspark.sql.functions import col
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import desc, row_number, monotonically_increasing_id
from pyspark.sql.window import Window
from pyspark.sql.functions import udf

# load up other dependencies
import re
import glob
#import optparse


# In[2]:


# configure spark variables
sc = SparkContext()
sqlContext = SQLContext(sc)
spark = SparkSession(sc)


# In[3]:


# Download NASA_access_log_Aug95.gz file
get_ipython().system(' wget ftp://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz')


# In[4]:


#insert data
raw_data_files = glob.glob('*.gz')
raw_data_files


# In[5]:


#produces a DataFrame with a single string column called value:
base_df = spark.read.text(raw_data_files)
base_df.printSchema()


# In[6]:


#convert a DataFrame into a Resilient Distributed Dataset (RDD)—Spark’s original data structure
base_df_rdd = base_df.rdd


# In[7]:


#the total number of logs of the dataset
print((base_df.count(), len(base_df.columns)))


# In[8]:


#extract and have a look of some sample log messages
sample_logs = [item['value'] for item in base_df.take(15)]
sample_logs


# In[9]:


# Extracting hostnames - with regular expressions extract the hostname from the logs
host_pattern = r'(^\S+\.[\S+\.]+\S+)\s'
hosts = [re.search(host_pattern, item).group(1)
           if re.search(host_pattern, item)
           else 'no match'
           for item in sample_logs]


# In[10]:


# Extracting timestamps - with regular expressions extract the timestamp fields from the logs
ts_pattern = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
timestamps = [re.search(ts_pattern, item).group(1) for item in sample_logs]


# In[11]:


# Extracting HTTP request method, URIs, and protocol - 
# with regular expressions extract the HTTP request methods, URIs, and Protocol patterns fields from the logs
method_uri_protocol_pattern = r'\"(\S+)\s(\S+)\s*(\S*)\"'
method_uri_protocol = [re.search(method_uri_protocol_pattern, item).groups()
               if re.search(method_uri_protocol_pattern, item)
               else 'no match'
              for item in sample_logs]


# In[12]:


# Extracting HTTP status codes - with regular expressions extract the HTTP status codes from the logs
status_pattern = r'\s(\d{3})\s'
status = [re.search(status_pattern, item).group(1) for item in sample_logs]


# In[13]:


# Extracting HTTP response content size - 
# with regular expressions extract the HTTP response content size from the logs
content_size_pattern = r'\s(\d+)$'
content_size = [re.search(content_size_pattern, item).group(1) for item in sample_logs]


# In[14]:


# Putting it all together - 
# We build our DataFrame with all of the log attributes neatly extracted in their own separate columns
logs_df = base_df.select(regexp_extract('value', host_pattern, 1).alias('host'),
                         regexp_extract('value', ts_pattern, 1).alias('timestamp'),
                         regexp_extract('value', method_uri_protocol_pattern, 1).alias('method'),
                         regexp_extract('value', method_uri_protocol_pattern, 2).alias('endpoint'),
                         regexp_extract('value', method_uri_protocol_pattern, 3).alias('protocol'),
                         regexp_extract('value', status_pattern, 1).cast('integer').alias('status'),
                         regexp_extract('value', content_size_pattern, 1).cast('integer').alias('content_size'))
logs_df.show(10, truncate=True)
print((logs_df.count(), len(logs_df.columns)))


# In[15]:


# Putting index column 
df_with_seq_id = logs_df.withColumn('index', row_number().over(Window.orderBy(monotonically_increasing_id())) - 1)
df_with_seq_id.show()


# In[16]:


# rows with potential null or missing values.
bad_rows_df = df_with_seq_id.filter(logs_df['host'].isNull()|
                             df_with_seq_id['timestamp'].isNull() |
                             df_with_seq_id['method'].isNull() |
                             df_with_seq_id['endpoint'].isNull() |
                             df_with_seq_id['status'].isNull() |
                             df_with_seq_id['content_size'].isNull()|
                             df_with_seq_id['protocol'].isNull())
bad_rows_df.count()


# In[17]:


# find out which columns contains malformed entries.
def count_null(col_name):
    return spark_sum(col(col_name).isNull().cast('integer')).alias(col_name)

# Build up a list of column expressions, one per column.
exprs = [count_null(col_name) for col_name in df_with_seq_id.columns]

# Run the aggregation. The *exprs converts the list of expressions into
# variable function arguments.
bad_rows_df.agg(*exprs).show()


# In[18]:


# Handling nulls in HTTP status
regexp_extract('value', r'\s(\d{3})\s', 1).cast('integer').alias( 'status')
null_status_df = base_df.filter(~base_df['value'].rlike(r'\s(\d{3})\s'))


# In[ ]:





# In[19]:


# pass this through the log data parsing pipeline
bad_status_df = null_status_df.select(regexp_extract('value', host_pattern, 1).alias('host'),
                                      regexp_extract('value', ts_pattern, 1).alias('timestamp'),
                                      regexp_extract('value', method_uri_protocol_pattern, 1).alias('method'),
                                      regexp_extract('value', method_uri_protocol_pattern, 2).alias('endpoint'),
                                      regexp_extract('value', method_uri_protocol_pattern, 3).alias('protocol'),
                                      regexp_extract('value', status_pattern, 1).cast('integer').alias('status'),
                                      regexp_extract('value', content_size_pattern, 1).cast('integer').alias('content_size'))


# In[20]:


# Handling nulls in HTTP content size
logs_df = df_with_seq_id[logs_df['status'].isNotNull()]
exprs = [count_null(col_name) for col_name in logs_df.columns]
logs_df.agg(*exprs).show()


# In[21]:


regexp_extract('value', r'\s(\d+)$', 1).cast('integer').alias('content_size')


# In[22]:


# find the records with potential missing content sizes in our base DataFrame
null_content_size_df = base_df.filter(~base_df['value'].rlike(r'\s\d+$'))
null_content_size_df.count()


# In[23]:


# Fix the rows with null content_size -  replace the null values in logs_df with 0 
logs_df = logs_df.na.fill({'content_size': 0})
exprs = [count_null(col_name) for col_name in logs_df.columns]
# the missing values in the content_size field with 0
logs_df.agg(*exprs).show()


# In[24]:


# Handling temporal fields (timestamp) - parse the timestamp field into an actual timestamp.
month_map = {
  'Jan': 1, 'Feb': 2, 'Mar':3, 'Apr':4, 'May':5, 'Jun':6, 'Jul':7,
  'Aug':8,  'Sep': 9, 'Oct':10, 'Nov': 11, 'Dec': 12
}

def parse_clf_time(text):
    """ Convert Common Log time format into a Python datetime object
    Args:
        text (str): date and time in Apache time format [dd/mmm/yyyy:hh:mm:ss (+/-)zzzz]
    Returns:
        a string suitable for passing to CAST('timestamp')
    """
    # NOTE: We're ignoring the time zones here, might need to be handled depending on the problem you are solving
    return "{0:04d}-{1:02d}-{2:02d} {3:02d}:{4:02d}:{5:02d}".format(
      int(text[7:11]),
      month_map[text[3:6]],
      int(text[0:2]),
      int(text[12:14]),
      int(text[15:17]),
      int(text[18:20])
    )


# In[25]:


# parse our DataFrame's time column.
udf_parse_time = udf(parse_clf_time)

logs_df = (logs_df.select('*', udf_parse_time(logs_df['timestamp'])
                                  .cast('timestamp')
                                  .alias('time'))
                  .drop('timestamp'))


# In[26]:


# verify by checking the DataFrame's schema.
logs_df.cache()


# In[27]:


# Initializing SparkSession
sc = SparkSession.builder.appName("PysparkExample").config ("spark.sql.shuffle.partitions", "50").config("spark.driver.maxResultSize","5g").config ("spark.sql.execution.arrow.enabled", "true").getOrCreate()


# In[28]:


# Registering a table
logs_df.registerTempTable("data_table")


# In[29]:


print ("q1: Top 10 requested pages and the number of requests made for each",'\n')

sql = """
select endpoint as pages, count (endpoint) as requests
from data_table 
where method = 'GET'
group by endpoint
order by requests desc
limit 10
"""

spark.sql(sql).show()


# In[30]:


print ("q2: Percentage of successful requests (anything in the 200s and 300s range)",'\n')

sql = """
select (select count(status) from data_table where status like '2%%' or status like '3%%') / (select count(status) from data_table) * 100 as Percentage_of_successful_requests
"""

spark.sql(sql).show()


# In[31]:


print ("q3: Percentage of unsuccessful requests (anything that is not in the 200s or 300s range)",'\n')

sql = """
select (select count(status) from data_table where status NOT LIKE  '2%%' and status NOT LIKE '3%%') / (select count(status) from data_table) * 100 as Percentage_of_successful_requests
"""

spark.sql(sql).show()


# In[32]:


print ("q4: Top 10 unsuccessful page requests",'\n')

sql = """
select endpoint as pages, count (endpoint) as requests
from data_table 
where  status NOT LIKE  '2%%' 
and status NOT LIKE '3%%'
and method = 'GET'
group by endpoint
order by requests desc
limit 10
"""

spark.sql(sql).show()


# In[33]:


print("q5: The top 10 hosts making the most requests, displaying the IP address and number of requests made.",'\n')

sql = """
select host, count (host) as requests
from data_table 
where method = 'GET'
group by host
order by requests desc
limit 10
"""

spark.sql(sql).show()


# In[34]:


print("q7 (1st try): For each of the top 10 hosts, show the top 5 pages requested and the number of requests for each page",'\n')

sql = """
select 
q.host as hosts,
q.requested as pages,
count(q.requested) as requests_number
from 
(select 
T.host as host,
T.endpoint as requested
from (
select T.host,T.endpoint,
row_number() over(partition by T.host order by T.endpoint desc) as rn
from data_table as T
where method = 'GET'
) as T
where T.rn <= 5) as q
group by q.host, q.requested
order by hosts,requests_number desc
"""

spark.sql(sql).show()


# In[217]:


print("q7 (2nd try): For each of the top 10 hosts, show the top 5 pages requested and the number of requests for each page",'\n')

sql = """
with rws as (
  select o.host, o.endpoint, row_number () over (
           partition by host
           order by endpoint desc
         ) rn
  from   data_table as o
)
  select * from rws
  where  rn <= 5
  
  order  by host, endpoint desc;
"""


spark.sql(sql).show()


# In[225]:


print("q7 (compare): For each of the top 10 hosts, show the top 5 pages requested and the number of requests for each page",'\n')

sql = """
select q.host, q.endpoint as page, count (q.host) as hosts_requests
from data_table as q
group by q.host, q.endpoint
ORDER BY hosts_requests desc
limit 10
"""

sql1 = """
select q.host,q.endpoint as page, count (q.endpoint) as page_requests
from data_table as q
group by  q.endpoint, q.host
ORDER BY page_requests desc
limit 10
"""



spark.sql(sql).show()
spark.sql(sql1).show()


# In[221]:


# counting malformed entries
bad_rows_df.count()


# In[226]:


# show all the malformed entries
bad_rows_df.show(10)


# In[37]:


# Registering a table with malformed entries
bad_rows_df.registerTempTable("bad_rows_data_table")


# In[38]:


#print("q8: The log file contains malformed entries; for each malformed line, display an error message and the line number.") 
#sql = """
#SELECT *
#FROM bad_rows_data_table 
#"""
#spark.sql(sql).show(100)


# In[227]:


print("q8: The log file contains malformed entries; for each malformed line, display an error message and the line number.,'\n'") 

sql = """
SELECT  case
when content_size is null
then 'field size is null'
when host is null
then 'field host is null'
when host is null
then 'field host is null'
when timestamp is null
then 'field timestamp is null'
when endpoint is null
then 'field endpoint is null'
when protocol = ' '
then 'field protocol is null'
when method is null
then 'field method is null'
else 'No errors'
end  as error_message , index, host, timestamp, endpoint, protocol, content_size as size
FROM bad_rows_data_table 
"""

spark.sql(sql).show(10)


# In[ ]:





# In[ ]:





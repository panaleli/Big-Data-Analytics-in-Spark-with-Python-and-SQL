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

import optparse



# In[2]:


sc = SparkContext()
sqlContext = SQLContext(sc)
spark = SparkSession(sc)


# In[3]:


# Download NASA_access_log_Aug95.gz file
#get_ipython().system(' wget ftp://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz')


# In[4]:


#insert data
raw_data_files = glob.glob('*.gz')
raw_data_files


# In[5]:


base_df = spark.read.text(raw_data_files)
#base_df.printSchema()


# In[6]:


base_df_rdd = base_df.rdd


# In[7]:


sample_logs = [item['value'] for item in base_df.take(15)]


# In[8]:


host_pattern = r'(^\S+\.[\S+\.]+\S+)\s'
hosts = [re.search(host_pattern, item).group(1)
           if re.search(host_pattern, item)
           else 'no match'
           for item in sample_logs]


# In[9]:


ts_pattern = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
timestamps = [re.search(ts_pattern, item).group(1) for item in sample_logs]


# In[10]:


method_uri_protocol_pattern = r'\"(\S+)\s(\S+)\s*(\S*)\"'
method_uri_protocol = [re.search(method_uri_protocol_pattern, item).groups()
               if re.search(method_uri_protocol_pattern, item)
               else 'no match'
              for item in sample_logs]


# In[11]:


status_pattern = r'\s(\d{3})\s'
status = [re.search(status_pattern, item).group(1) for item in sample_logs]


# In[12]:


content_size_pattern = r'\s(\d+)$'
content_size = [re.search(content_size_pattern, item).group(1) for item in sample_logs]


# In[13]:


logs_df = base_df.select(regexp_extract('value', host_pattern, 1).alias('host'),
                         regexp_extract('value', ts_pattern, 1).alias('timestamp'),
                         regexp_extract('value', method_uri_protocol_pattern, 1).alias('method'),
                         regexp_extract('value', method_uri_protocol_pattern, 2).alias('endpoint'),
                         regexp_extract('value', method_uri_protocol_pattern, 3).alias('protocol'),
                         regexp_extract('value', status_pattern, 1).cast('integer').alias('status'),
                         regexp_extract('value', content_size_pattern, 1).cast('integer').alias('content_size'))
#logs_df.show(10, truncate=True)
#print((logs_df.count(), len(logs_df.columns)))


# In[14]:


df_with_seq_id = logs_df.withColumn('index', row_number().over(Window.orderBy(monotonically_increasing_id())) - 1)
#df_with_seq_id.show()


# In[15]:


bad_rows_df = df_with_seq_id.filter(logs_df['host'].isNull()|
                             df_with_seq_id['timestamp'].isNull() |
                             df_with_seq_id['method'].isNull() |
                             df_with_seq_id['endpoint'].isNull() |
                             df_with_seq_id['status'].isNull() |
                             df_with_seq_id['content_size'].isNull()|
                             df_with_seq_id['protocol'].isNull())                            


# In[16]:


def count_null(col_name):
    return spark_sum(col(col_name).isNull().cast('integer')).alias(col_name)

# Build up a list of column expressions, one per column.
exprs = [count_null(col_name) for col_name in df_with_seq_id.columns]

# Run the aggregation. The *exprs converts the list of expressions into
# variable function arguments.
#logs_df.agg(*exprs).show()


# In[17]:


regexp_extract('value', r'\s(\d{3})\s', 1).cast('integer').alias( 'status')


# In[18]:


null_status_df = base_df.filter(~base_df['value'].rlike(r'\s(\d{3})\s'))


# In[19]:


bad_status_df = null_status_df.select(regexp_extract('value', host_pattern, 1).alias('host'),
                                      regexp_extract('value', ts_pattern, 1).alias('timestamp'),
                                      regexp_extract('value', method_uri_protocol_pattern, 1).alias('method'),
                                      regexp_extract('value', method_uri_protocol_pattern, 2).alias('endpoint'),
                                      regexp_extract('value', method_uri_protocol_pattern, 3).alias('protocol'),
                                      regexp_extract('value', status_pattern, 1).cast('integer').alias('status'),
                                      regexp_extract('value', content_size_pattern, 1).cast('integer').alias('content_size'))


# In[20]:


logs_df = df_with_seq_id[logs_df['status'].isNotNull()]
exprs = [count_null(col_name) for col_name in logs_df.columns]
#logs_df.agg(*exprs).show()


# In[21]:


regexp_extract('value', r'\s(\d+)$', 1).cast('integer').alias('content_size')


# In[22]:


null_content_size_df = base_df.filter(~base_df['value'].rlike(r'\s\d+$'))
#null_content_size_df.count()


# In[23]:


logs_df = logs_df.na.fill({'content_size': 0})
exprs = [count_null(col_name) for col_name in logs_df.columns]
#logs_df.agg(*exprs).show()


# In[24]:


#logs_df.show()


# In[25]:



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


# In[26]:


udf_parse_time = udf(parse_clf_time)

logs_df = (logs_df.select('*', udf_parse_time(logs_df['timestamp'])
                                  .cast('timestamp')
                                  .alias('time'))
                  .drop('timestamp'))


# In[27]:


#logs_df.show(10, truncate=True)


# In[28]:


#logs_df.printSchema()


# In[29]:


logs_df.cache()


# In[30]:


# Initializing SparkSession
sc = SparkSession.builder.appName("PysparkExample").config ("spark.sql.shuffle.partitions", "50").config("spark.driver.maxResultSize","5g").config ("spark.sql.execution.arrow.enabled", "true").getOrCreate()


# In[31]:


# Registering a table
logs_df.registerTempTable("data_table")


# In[32]:


#sc.sql("select * from data_table").show(20)


# In[39]:


def q1():
    #print ("Top 10 requested pages and the number of requests made for each")

    sql = """
    select endpoint as pages, count (endpoint) as requests
    from data_table 
    where method = 'GET'
    group by endpoint
    order by requests desc
    limit 10
    """

    return spark.sql(sql).show()


# In[ ]:





# In[41]:


def q2():
    #print ("Percentage of successful requests (anything in the 200s and 300s range)")

    sql = """
    select (select count(status) from data_table where status like '2%%' or status like '3%%') / (select count(status) from data_table) * 100 as Percentage_of_successful_requests
    """


    return spark.sql(sql).show()


# In[42]:


def q3():
    #print ("Percentage of unsuccessful requests (anything that is not in the 200s or 300s range)")

    sql = """
    select (select count(status) from data_table where status NOT LIKE  '2%%' and status NOT LIKE '3%%') / (select count(status) from data_table) * 100 as Percentage_of_successful_requests
    """


    return spark.sql(sql).show()


# In[43]:


def q4():
    #print ("Top 10 unsuccessful page requests")

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

    return spark.sql(sql).show()


# In[44]:


def q5():
    #print("The top 10 hosts making the most requests, displaying the IP address and number of requests made.")

    sql = """
    select host, endpoint,count (endpoint) as requests
    from data_table 
    where method = 'GET'
    group by endpoint, host
    order by requests desc
    limit 10
    """

    return spark.sql(sql).show()


# In[ ]:





# In[45]:

#print ("Start the Parser!!!!",'\n')

#import argparse# Create the parser


#def fib(sql):
#    return spark.sql(sql).show()

# Import the library

#parser = argparse.ArgumentParser()# Add an argument
#parser.add_argument('--sql', type=str, required=True)# Parse the argument
#args = parser.parse_args()
#(options, args) = parser.parse_args()

#result = fib(options.sql)





#def fib(sql):
#    return spark.sql(sql).show()







def Main():
    parser = optparse.OptionParser('usage'+'--q1 or --q2 or --q3 etc. <Question number. Ex. q1, q2, etc>' , version="%prog 1.0")
    
    parser.add_option('--q1', dest='q1', type='string',help='q1: Top 10 requested pages and the number of requests made for each')
    parser.add_option('--q2', dest='q2', type='string',help='q2: Percentage of successful requests (anything in the 200s and 300s range)')
    parser.add_option('--q3', dest='q3', type='string',help='q3: Percentage of unsuccessful requests (anything that is not in the 200s or 300s range)')
    parser.add_option('--q4', dest='q4', type='string',help='q4: Top 10 unsuccessful page requests')
    parser.add_option('--q5', dest='q5', type='string',help='q5: The top 10 hosts making the most requests, displaying the IP address and number of requests made.')
    
        
    (options, args) = parser.parse_args()
   
     
    if (options.q1 != None):
        print ('\n','q1: Top 10 requested pages and the number of requests made for each','\n')
        #sql = options.q1
        result = q1()
        
    if (options.q2 != None):
        print ('\n','q2: Percentage of successful requests (anything in the 200s and 300s range)','\n')
        #sql = options.q2
        result = q2()        
    
    if (options.q3 != None):
        print ('\n','q3: Percentage of unsuccessful requests (anything that is not in the 200s or 300s range)','\n')
        #sql = options.q3
        result = q3()  
        
    if (options.q4 != None):
        print ('\n','q4: Top 10 unsuccessful page requests','\n')
        #sql = options.q4
        result = q4()  
        
    if (options.q5 != None):
        print ('\n','q5: The top 10 hosts making the most requests, displaying the IP address and number of requests made.','\n')
        #sql = options.q5
        result = q5()                      
       
    else:
        print (parser.usage)
        exit(0)


if __name__ == '__main__':
    Main()






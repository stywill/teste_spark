Hadoop

hadoop fs -put access_log_Aug95 /user/cloudera/nasa
hadoop fs -put access_log_Jul95 /user/cloudera/nasa


Hive

CREATE EXTERNAL TABLE logs_nasa  
(host string,data_a string,requisicao string ,retorno_http int ,  
total_bytes int)
ROW FORMAT DELIMITED FIELDS TERMI  NATED BY ' '  
LOCATION '/user/cloudera/nasa/'


Spark

from pyspark.sql import HiveContext
from pyspark import SparkContext
import pyspark.sql.functions as F
from pyspark.sql.functions import *

h = HiveContext(sc)
df = h.sql("select * From data_set.logs_nasa")
df.show()

1. Número de hosts únicos.
	hostsUnicos = df.groupBy("host").count()
	hostsUnicos.show()
	R: 137979 
 
2. O total de erros 404.
	erros404 = df.filter(df["retorno_http"]==404)
	erros404.count()
	R: 20725

3. Os 5 URLs que mais causaram erro 404.
	urlsErro = df.filter(df["retorno_http"]==404).groupBy("requisicao").count()
	maioresCausadores404 = urlsErro.orderBy(urlsErro["count"],ascending=0).limit(5)
	maioresCausadores404.show()
	+--------------------+-----+                                                    
	|          requisicao|count|
	+--------------------+-----+
	|GET/pub/winvn/rea...| 2004|
	|GET/pub/winvn/rel...| 1732|
	|GET/shuttle/missi...|  682|
	|GET/shuttle/missi...|  426|
	|GET/history/apoll...|  384|
	+--------------------+-----+

4. Quantidade de erros 404 por dia.
	df2 = df.select(df["host"],substring(df["data_a"],1,11).alias('data'),df["requisicao"],df["retorno_http"],df["total_bytes"])
	urls404 = df2.filter(df2["retorno_http"]==404).groupBy(df2["data"]).count()
	urls404Dia = urls404.filter(urls404["data"]=="29/Aug/1995")
	urls404Dia.show()
	+-----------+-----+
	|       data|count|
	+-----------+-----+
	|29/Aug/1995|  411|
	+-----------+-----+

5. O total de bytes retornados.
	totalBytes = df.groupBy().agg(F.sum("total_bytes").alias('bytes_retornados'))
	totalBytes.show()
	+----------------+                                                              
	|bytes_retornados|
	+----------------+
	|     65253787975|
	+----------------+
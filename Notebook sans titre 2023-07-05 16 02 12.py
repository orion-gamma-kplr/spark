# Databricks notebook source
import requests 
from datetime import datetime
import time

# Déclaration des variables
API_key="TQLL3YQZLLXNVHGK"
stocks=('AMZN','AAPL','GOOG','MSFT')
#url_api=f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=STOCK&interval=1min&apikey={API_key}&datatype=csv"
#dataSchema = "timestamp timestamp, open float, high float, low float, close float,volume integer"
url_api=f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=STOCK&apikey={API_key}&datatype=csv"
dataSchema = "symbol string, open float, high float, low float, price float,volume integer,latestDay date,previousClose float,change float,changePercent float"

# COMMAND ----------

# On arrete tous les streams
for s in spark.streams.active:
  s.stop()
  
# Parcours de la liste des actions
for stock in stocks:
# On supprime les répertoires
    print(f"{stock}")
    dbutils.fs.rm(f"dbfs:/mnt/{stock}/",True)

# Parcours de la liste des actions
for stock in stocks:
# On tente de créer les répertoires
    try:
        dbutils.fs.mkdirs(f"dbfs:/mnt/{stock}")
    except:
        print(f"{stocks[i]} existe déjà")

# COMMAND ----------

# Parcours de la liste des actions
#while True:
for stock in stocks:
# On appele l'api
    print(f"{stock}")
    response = requests.get(url_api.replace("STOCK",stock.upper()))
# si tout ce passe bien
    if response.ok:
# On enregistre le fichier CSV dans le répertoire concerné
        file_name=f"dbfs:/mnt/{stock}/{stock}-{datetime.now().strftime('%Y%m%d-%H%M')}.csv"
        print(file_name)
        print(response.text.replace("%",""))
        dbutils.fs.put(file_name,response.text.replace("%",""))

        df_stock = (spark.read
                    .option("header", True)
                    .option("inferSchema", True)
                    .csv(file_name))
        display(df_stock)
# On attend 1 minute avant de recommencer
#    time.sleep(60)

# COMMAND ----------

# On crèe les readstreams 
dataPath = f"dbfs:/mnt/GOOG"
DF_GOOG = (spark
    .readStream                            # Returns DataStreamReader
    .option("maxFilesPerTrigger", 1) 
    .option("header",True)      # Force processing of only 1 file per trigger
    .schema(dataSchema)                    # Required for all streaming DataFrames
    .csv(dataPath)                        # The stream's source directory and file type
)

# On crèe les writestreams 
outputPathDir = dataPath + "/output.parquet"                  # A subdirectory for our output
checkpointPath = dataPath + "/checkpoint"                     # A subdirectory for our checkpoint & W-A logs

streamingQuery = (DF_GOOG                                      # Start with our "streaming" DataFrame
  .writeStream                                                # Get the DataStreamWriter
  .queryName("stream_GOOG")                                     # Name the query
  .trigger(processingTime="1 minute")                        # Configure for a 3-second micro-batch
  .format("parquet")                                          # Specify the sink type, a Parquet file
  .option("checkpointLocation", checkpointPath)               # Specify the location of checkpoint files & W-A logs
  .outputMode("append")                                       # Write only new data to the "file"
  .start(outputPathDir)                                       # Start the job, writing to the specified directory
)

# COMMAND ----------

# Parcours de la liste des actions
dbutils.fs.ls("dbfs:/mnt/GOOG/output.parquet/")
#dbutils.fs.ls("dbfs:/mnt/AMZN/")
#dbutils.fs.ls("dbfs:/mnt/AAPL/")
#dbutils.fs.ls("dbfs:/mnt/MSFT/")

# COMMAND ----------

for s in spark.streams.active:         # Iterate over all streams
  print("{}: {}".format(s.id, s.name)) # Print the stream's id and name

# COMMAND ----------

# On charge le parquet 
stock_GOOG = spark.read.parquet(outputPathDir)
# On affiche la structure et le contenu
stock_GOOG.printSchema()
display(stock_GOOG)
# Nombre de lignes
stock_GOOG.count()
stock_GOOG.select('latestDay').distinct().count()
stock_GOOG.write.mode('overwrite').saveAsTable("stock_GOOG")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from stock_GOOG 

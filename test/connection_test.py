import os
from databricks import sql
from databricks.sql.types import Row
from dotenv import load_dotenv

load_dotenv()

host = os.getenv("DATABRICKS_SERVER_HOSTNAME")
http_path = os.getenv("DATABRICKS_HTTP_PATH")
access_token = os.getenv("DATABRICKS_TOKEN")

connection = sql.connect(
  server_hostname=host,
  http_path=http_path,
  access_token=access_token)

cursor = connection.cursor()

cursor.execute('SELECT * FROM user_tab LIMIT 10')
result: list[Row] = cursor.fetchall()
for row in result:
  print(row.asDict(recursive=True))

cursor.close()
connection.close()
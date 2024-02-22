import pandas as pd
import time
from elasticsearch import Elasticsearch

start_time = time.time()

str1 = input("where are your first data?")
str2 = input("where are your second data?")
df1 = pd.read_csv(str1)
df2 = pd.read_csv(str2)
str3 = input("what is your elasticsearch index?")

es = Elasticsearch('http://localhost:9200')

index_name = input("what is your elasticsearch index?")

query = {
    "query": {
        "match_all": {}
    }
}

result = es.search(index=index_name, body=query)
data = [hit['_source'] for hit in result['hits']['hits']]
dfEla = pd.DataFrame(data)

onWhat = input("what do you want to merge your data on?")
merged_df1 = pd.merge(df1, df2, on=onWhat, how='inner')
merged_df1 = pd.merge(df1, df2, on=onWhat, how='inner')
merged_df_result = pd.merge(merged_df1, dfEla, on=onWhat, how='inner')


merged_df_result.to_csv('merged_file.csv', index=False)

end_time = time.time()

elapsed_time = end_time - start_time
print(f"Time taken: {elapsed_time:.2f} seconds")

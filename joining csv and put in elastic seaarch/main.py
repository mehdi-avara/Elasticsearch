import pandas as pd
from elasticsearch import Elasticsearch, helpers

df1 = pd.read_csv('x.csv').head(500)

df2 = pd.read_csv('y.csv').head(500)

result = pd.merge(df1, df2, on='User Id', how='inner')

es = Elasticsearch('http://localhost:9200')


query = {
    "query": {
        "match_all": {}
    },
    "size": 500  
}

es_results = es.search(index="join_table1", body=query)
es_data = [hit['_source'] for hit in es_results['hits']['hits']]

es_df = pd.DataFrame(es_data)

final_result = pd.merge(result, es_df, on='User Id', how='inner')

csv_file_path = 'final_result.csv'
final_result.to_csv(csv_file_path, index=True)

index_name = '1'

for idx, row in final_result.iterrows():
    doc = row.to_dict()
    es.index(index=index_name, id=idx + 1, body=doc)

es.indices.refresh(index=index_name)

from flask import Flask, jsonify
from elasticsearch import Elasticsearch

app = Flask(__name__)
es = Elasticsearch('http://localhost:9200')  

index_name = 'news'  

@app.route('/api/data', methods=['GET'])
def get_data():
    response = es.search(index=index_name, body={"query": {"match_all": {}}})

    hits = response['hits']['hits']
    data_list = [{'topic': hit['_source']['topic'],
                  'address': hit['_source']['address'],
                  'src': hit['_source']['src'],
                  'date': hit['_source']['date']} for hit in hits]

    return jsonify(data_list)

if __name__ == '__main__':
    app.run(debug=True)

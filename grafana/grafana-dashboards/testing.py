from elasticsearch import Elasticsearch

es = Elasticsearch("http://localhost:9200")

response = es.search(
    index="ddos_predictions",
    size=0,  # We don't need the actual documents, just the aggregation
    body={
        "aggs": {
            "avg_confidence": {
                "avg": {
                    "field": "confidence"
                }
            }
        }
    }
)

avg_confidence = response["aggregations"]["avg_confidence"]["value"]
print("Average confidence:", avg_confidence)
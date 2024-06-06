from app.utils.mongo_db_connector import MongoDBConnector

mongo_connector = MongoDBConnector()

query = [
    {
        '$match': {
            'id': sdfg34tgdfgd",
            'is_root': True,
            'series_number': {'$in': [1,2,3,4,5]}
        }
    }
]

mycol = mongo_connector.get_collection('my_collection')
query_output = list(mycol.aggregate(query))

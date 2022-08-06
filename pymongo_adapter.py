import pymongo
from bson import json_util


class MongoDBService:
    # Name of the database
    MONGODB_DATABASE = "mongo_db"

    def __init__(self, host="localhost", port=27017, db_name=None, page_size=100):
        self.host = host
        self.port = port
        # page_size is used to limit the number of records returned in a single query
        self.limit = page_size
        if db_name is not None:
            self.db_name = db_name
        else:
            self.db_name = self.MONGODB_DATABASE

    def connect(self, collection_name: str) -> pymongo.collection.Collection:
        """
        Returns mongo collection object for the given collection name
        """
        client = pymongo.MongoClient(self.host, self.port)
        db = client[self.db_name]
        if collection_name:
            return db[collection_name]
        return None

    def count(self, collection_name: str, query: dict) -> int:
        """
        Returns count of the data in the collection
        """
        collection = self.connect(collection_name)
        return collection.count_documents(query)

    def distinct(self, collection_name: str, key: str) -> list:
        """
        Returns list of distinct values of the field
        """
        collection = self.connect(collection_name)
        result = collection.distinct(key)
        if None in result:
            result.remove(None)
        return result

    def indexes(self, collection_name: str) -> list:
        """
        Returns list of indexes of the collection
        """
        _result = []
        collection = self.connect(collection_name)
        indexes = collection.index_information()
        for key, value in indexes.items():
            _result.extend([x[0] for x in value["key"]])
        return _result

    def create_index(
        self, collection_name: str, keys: list, case_insensitive: bool = False
    ) -> str:
        """
        Creates index on the collection
        """
        collection = self.connect(collection_name)
        # For case insensitive indexing, collation must be set to 1 or 2
        # https://www.mongodb.com/developer/products/mongodb/schema-design-anti-pattern-case-insensitive-query-index/
        _collation = pymongo.collation.Collation(locale="en", strength=2)
        keys = [(x, pymongo.ASCENDING) for x in keys]
        if case_insensitive:
            return collection.create_index(keys, collation=_collation)
        return collection.create_index(keys)

    def find(
        self,
        collection_name: str,
        query: dict,
        _limit: int,
        _skip: int = 0,
        case_insensitive: bool = False,
    ) -> list:
        """
        Returns data from the collection
        """
        collection = self.connect(collection_name)
        if case_insensitive:
            _collation = pymongo.collation.Collation(locale="en", strength=2)
            data = (
                collection.find(query, {"_id": False})
                .collation(_collation)
                .limit(_limit)
                .skip(_skip)
            )
        else:
            data = collection.find(query, {"_id": False}).skip(_skip).limit(_limit)
        return json_util.dumps(data)

    def get_data(
        self,
        collection_name: str,
        query: dict,
        page: int = 1,
        limit: int = None,
        case_insensitive: bool = False,
    ) -> list:
        """
        Return data from the collection in paginated format
        """
        limit = self.limit if limit is None else limit
        _skip = (page - 1) * limit
        return self.find(collection_name, query, limit, _skip, case_insensitive)

    def insert_many(self, collection_name: str, data: list) -> list:
        """
        Inserts data into the collection and returns list of inserted ids
        """
        collection = self.connect(collection_name)
        return collection.insert_many(data).inserted_ids

    def insert_one(self, collection_name: str, data: dict) -> str:
        """
        Inserts data into the collection and returns inserted id
        """
        collection = self.connect(collection_name)
        return collection.insert_one(data).inserted_id

    def update_many(self, collection_name: str, query: dict, data: dict) -> int:
        """
        Updates data in the collection and returns number of updated records
        """
        collection = self.connect(collection_name)
        return collection.update_many(query, data).modified_count

    def update_one(self, collection_name: str, query: dict, data: dict) -> int:
        """
        Updates data in the collection and returns number of updated records
        """
        collection = self.connect(collection_name)
        return collection.update_one(query, data).modified_count

    def delete_many(self, collection_name: str, query: dict) -> int:
        """
        Deletes data from the collection and returns number of deleted records
        """
        collection = self.connect(collection_name)
        return collection.delete_many(query).deleted_count

    def delete_one(self, collection_name: str, query: dict) -> int:
        """
        Deletes data from the collection and returns number of deleted records
        """
        collection = self.connect(collection_name)
        return collection.delete_one(query).deleted_count

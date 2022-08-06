"""
This is the sample usecase of the this library, it 
demonstrates how to use the library. It may not the 
best way to use the library, we will improve it in 
the future.
"""

from .pymongo_adapter import MongoDBService

db_service = MongoDBService(db_name="mongo_db")
collection_name = "sample_collection"


sample_data = [
    {"name": "John", "age": 30, "city": "New York"},
    {"name": "Jane", "age": 25, "city": "Paris"},
    {"name": "Jack", "age": 27, "city": "London"},
    {"name": "Jill", "age": 22, "city": "New York"},
]

query = {"name": "john"}

if __name__ == "__main__":
    # Writing bulk data to the database
    db_service.insert_many(collection_name, sample_data)
    print("Inserted bulk data into the collection")

    # Writing a single data point to the database
    db_service.insert_one(collection_name, {"name": "Arron", "age": 24, "city": "Pune"})
    print("Inserted single data point")

    # Reading data from the database
    result = db_service.get_data(collection_name, query, page=1)
    print(f"Result of the query {query}:", result)

    # Counting data in the database
    count = db_service.count(collection_name, query)
    print(f"Count of the query {query}:", count)

    # Getting distinct values of the field
    distinct_values = db_service.distinct(collection_name, "city")
    print(f"Distinct values of the field city:", distinct_values)

    # Creating index on the collection
    db_service.create_index(collection_name, ["name", "city"], case_insensitive=True)
    print("Created index on the collection")

    # Getting indexes of the collection
    indexes = db_service.indexes(collection_name)
    print(f"Indexes of the collection:", indexes)

    # Deleting the collection
    db_service.drop_collection(collection_name)
    print("Deleted the collection")

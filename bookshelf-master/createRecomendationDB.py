from azure.cosmos import exceptions, CosmosClient, PartitionKey
import pickle

def get_recommendations():
    with open("../recommendations.pkl", "rb") as pkl_handle:
	    output = pickle.load(pkl_handle)
    

    recommendations = []
    for user_id, articles in output.items():
        user = user_id
        items = []
        for item, rates in articles:
            items.append(item)
        recommendations.append({"id" : str(user),
                                "user_id":user,
                                "articles": items})
    print("recommendations[:10]", recommendations[:10])
    return recommendations
    
# Initialize the Cosmos client
endpoint = "https://db-p9cosmos.documents.azure.com:443/"
key = 'b2YQGIzoUAUsvXcRwuEwjoVpSFUeEPmehpliLgyOFWLDFTQjkccj099TAG0YinPAu0Exa9X5d9NodM23NYZxJw=='

# <create_cosmos_client>
client = CosmosClient(endpoint, key)
# </create_cosmos_client>

# Create a database
# <create_database_if_not_exists>
database_name = 'RecommendationDB'
database = client.create_database_if_not_exists(id=database_name)
# </create_database_if_not_exists>

# Create a container
# Using a good partition key improves the performance of database operations.
# <create_container_if_not_exists>
container_name = 'RecommendationContainer'
container = database.create_container_if_not_exists(
    id=container_name, 
    partition_key=PartitionKey(path="/user_id"),
    offer_throughput=400
)
# </create_container_if_not_exists>


# Add items to the container
recommendation_items_to_create = get_recommendations()

 # <create_item>
for recommendation_item in recommendation_items_to_create:
    container.create_item(body=recommendation_item)
# </create_item>

# Read items (key value lookups by partition key and id, aka point reads)
# <read_item>
for recommendation in recommendation_item:
    item_response = container.read_item(item=recommendation['id'], partition_key=recommendation['user_id'])
    request_charge = container.client_connection.last_response_headers['x-ms-request-charge']
    print('Read item with id {0}. Operation consumed {1} request units'.format(item_response['id'], (request_charge)))
# </read_item>
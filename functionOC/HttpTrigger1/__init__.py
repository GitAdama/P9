import logging
import json
import azure.functions as func
from azure.cosmos import exceptions, CosmosClient, PartitionKey
import pickle
import os
import pandas as pd
import numpy as np
from surprise import Reader, Dataset, SVD
from surprise.model_selection import train_test_split
from collections import defaultdict
from tqdm import tqdm
from joblib import load



def get_top_recommendations(predictions, topN = 5):
         
        top_recom = defaultdict(list)
        for user_id, article_id, true_rating, estimated_rating, _ in tqdm(predictions, desc='• Recommendations gathering • '):
            top_recom[user_id].append((article_id, estimated_rating))
        
        for user_id, user_rating in tqdm(top_recom.items(), desc='• Recommendations sorting • '):
            user_rating.sort(key = lambda x: x[1], reverse = True)
            top_recom[user_id] = user_rating[:topN]
        
        return top_recom

def format_top_recommendations(top_recom, user_id, cat_map):
        recom_df = pd.DataFrame(columns=['articles','categories', 'est_rating'])
        for article in top_recom[user_id]:
            recom_df.loc[recom_df.shape[0]] = [
                article[0], 
                cat_map[cat_map['article_id'] == article[0]]['category_id'].min(), 
                article[1]
                ]
        recom_df['articles'] = recom_df['articles'].astype('int32')
        recom_df['categories'] = recom_df['categories'].astype('int16')
        return recom_df

def formatUserDf(row):
    ref_row = row.copy()
    row_to_concat = []
    for i, (article, score) in enumerate(row["article_id"].items()):
        new_row = ref_row
        if i == 0 :
            row["score"] = score
            row["article_id"] = article
            row_to_concat.append(row)
        else:
            new_row["score"] = score
            new_row["article_id"] = article
            row_to_concat.append(new_row)
    return pd.concat(row_to_concat, axis=1)


def create_recom(user_obj):
    pred_df = pd.read_csv("./predict_df.csv")
    pred_df = pred_df.sample(1000)
    # return pred_df
    # print("CURRENT_DIR =>", os.getcwd(), os.listdir("./"))
    user_df = pd.DataFrame(user_obj, columns=["user_id","article_id","score"])
    user_df = pd.concat([formatUserDf(row) for _, row in user_df.iterrows()], ignore_index=True, axis=1).T
    
    recom_def = pred_df.append(user_df, ignore_index = True)
    recom_def
    
    # {"id" : str(user),
    # "user_id":user,
    # "articles": items}
    top_recom = get_pred(recom_def)

    recommendations = []
    users = user_df.user_id.unique().tolist()
    for user_id, articles in top_recom.items():
        if user_id in users:
            user = user_id
            items = []
            for item, rates in articles:
                items.append(item)
            recommendations.append({"id" : str(user),
                                    "user_id":user,
                                    "articles": items})
    for i, (user_id, user_rating )in enumerate(top_recom.items()):
        if user_id in users:
            user = user_id
            items = []
            print("TOP_RECOMMENDATON =>", "user_id =>",user_id," recommendations =>", [article_id for (article_id, _) in user_rating])
        else:
            continue
    print("NEW_USER_LIST =>", users)
    # for i, (user_id, user_rating )in enumerate(top_recom.items()):
    #     if i <3:
    #         print("TYPE user_id=>", type(user_id))
    #         print("TYPE userlist=>", [type(x) for x in users])

    #         # user = user_id
    #         # items = []
    #         # print("TOP_RECOMMENDATON =>", "user_di =>",user_id," recommendations =>", [article_id for (article_id, _) in user_rating])
    #     else:
    #         break
                            
    return recommendations

def get_pred(recom_def):

    reader = Reader(rating_scale=(1, 5))
    data_ = Dataset.load_from_df(recom_def[['user_id', 'article_id', 'score']], reader)
    # trainset, testset = train_test_split(data_testa, test_size=.25)
    trainset = data_.build_full_trainset()
    algo = SVD()
    algo.fit(trainset)
    testset = trainset.build_anti_testset()
    predictions = algo.test(testset)

    

    print('Number of users: ', trainset.n_users, '\n')
    print('Number of items: ', trainset.n_items, '\n')

    predictions = algo.test(testset)
    top_recom = get_top_recommendations(predictions)
    # for i, (user_id, user_rating )in enumerate(top_recom.items()):
    #     if i<15:
    #         # for rec, it in articles :
    #         #     print("TOP_RECOMMENDATON =>", "user_di =>", user_id ," recommendations =>", rec)
    #         print("TOP_RECOMMENDATON =>", "user_di =>",user_id," recommendations =>", [article_id for (article_id, _) in user_rating])
    #     else:
    #         break

    return top_recom

# def new_article(article_id, ):
#     articleContainer = createDB("ArticleDB", "ArticleContainer", "/article_id")
#     article_query = list(articleContainer.query_items(
#         query="SELECT * FROM c WHERE c.user_id = @userId",
#         parameters=[dict(name="@userId", value=article_id)],
#         enable_cross_partition_query=True
#     ))
#     if len(article_query)<=0:
#         return "article not found !"
#     else:
#         model = load('./model_rec.joblib') 

#         pred_df = pd.read_csv("./predict_df.csv")
#         article_id_ = article_id
#         unique_users = pred_df.user_id.unique().tolist()
#         users_pred = [uid ,(model.predict(uid=article_id, iid=str(iid)).est) for iid in unique_users][:5]
#         user_to_recommend_to = json.dumps([x[1] for x in users_pred])
#         return user_to_recommend_to


def createDB(db_id, container_id, path):
    endpoint = "https://db-p9cosmos.documents.azure.com:443/"
    key = 'b2YQGIzoUAUsvXcRwuEwjoVpSFUeEPmehpliLgyOFWLDFTQjkccj099TAG0YinPAu0Exa9X5d9NodM23NYZxJw=='

    # <create_cosmos_client>
    client = CosmosClient(endpoint, key)
    # </create_cosmos_client>

    # Create a database
    # <create_database_if_not_exists>
    # database_name = 'RecommendationDB'
    database =  client.create_database_if_not_exists(id=db_id)
    # </create_database_if_not_exists>

    # Create a container
    # Using a good partition key improves the performance of database operations.
    # <create_container_if_not_exists>
    # container_name = 'RecommendationContainer'
    container = database.create_container_if_not_exists(
        id=container_id, 
        partition_key=PartitionKey(path=path),
        offer_throughput=400
    )

    return container

def main(req: func.HttpRequest) -> func.HttpResponse:
    
    logging.info('Python HTTP trigger function processed a request.')

    # endpoint = "https://db-p9cosmos.documents.azure.com:443/"
    # key = 'b2YQGIzoUAUsvXcRwuEwjoVpSFUeEPmehpliLgyOFWLDFTQjkccj099TAG0YinPAu0Exa9X5d9NodM23NYZxJw=='

    # # <create_cosmos_client>
    # client = CosmosClient(endpoint, key)
    # # </create_cosmos_client>

    # # Create a database
    # # <create_database_if_not_exists>
    # database_name = 'RecommendationDB'
    # database =  client.create_database_if_not_exists(id=database_name)
    # # </create_database_if_not_exists>

    # # Create a container
    # # Using a good partition key improves the performance of database operations.
    # # <create_container_if_not_exists>
    # container_name = 'RecommendationContainer'
    # container = database.create_container_if_not_exists(
    #     id=container_name, 
    #     partition_key=PartitionKey(path="/user_id"),
    #     offer_throughput=400
    # )


    ## Test UserDB and Articles 

    # <create_cosmos_client>
    # client = CosmosClient(endpoint, key)
    # </create_cosmos_client>

    # Create a database
    # <create_database_if_not_exists>
    # database_name = 'UserDB'
    # UserDatabase =  client.create_database_if_not_exists(id='UserDB')
    # </create_database_if_not_exists>

    # Create a container
    # Using a good partition key improves the performance of database operations.
    # <create_container_if_not_exists>
    # container_name = 'UserContainer'
    # UserContainer = UserDatabase.create_container_if_not_exists(
    #     id='UserContainer', 
    #     partition_key=PartitionKey(path="/user_id"),
    #     offer_throughput=400
    # )



    # recommendationContainer = createDB("RecommendationDB", "RecommendationContainer", "/user_id")              
    # items = list(recommendationContainer.query_items(
    #     query="SELECT * FROM c.user_id WHERE c",
    #     parameters=[dict(name="@userId", value=userId)],
    #     enable_cross_partition_query=True
    # ))
    # userContainer = createDB("UserDB", "UserContainer", "/user_id")
    # user_list = list(recommendationContainer.query_items(
    #         query="SELECT c.user_id FROM c",
    #         enable_cross_partition_query=True
    #     ))
    
    # articleContainer = createDB("ArticleDB", "ArticleContainer", "/article_id")
    # article_list = list(recommendationContainer.query_items(
    #         query="SELECT c.article_id FROM c ",
    #         enable_cross_partition_query=True
    #     ))
    # items_list = list(ArticleContainer.query_items(
    #         query="SELECT c.article_id FROM c",
    #         enable_cross_partition_query=True
    #     )) 

    # unique_users = np.unique([y  for x in user_list for y in x.values() ]).tolist()
    # unique_articles = np.unique([z  for y in article_list for x in y.values() for z in x ]).tolist()

    # print("LIST_USERS: UNIQUE_USERS =>", unique_users)
    # print("LIST_ARTICLES: UNIQUE_ARTICLES =>", unique_articles)

    # articleDatabase =  client.create_database_if_not_exists(id='ArticleDB')
    # </create_database_if_not_exists>

    # Create a container
    # Using a good partition key improves the performance of database operations.
    # <create_container_if_not_exists>
    # container_name = 'ArticleContainer'
    # articleContainer = articleDatabase.create_container_if_not_exists(
    #     id='ArticleContainer', 
    #     partition_key=PartitionKey(path="/article_id"),
    #     offer_throughput=400
    # )



    # Recom_con = createDB("RecommendationDB", "RecommendationContainer", "/user_id")
    # article_list = list(articleContainer.query_items(
    #         query="SELECT * FROM c ",
    #         enable_cross_partition_query=True
    #     ))

    # print("USER_LIST =>", user_list, "\n ARTICLE_LIST =>", article_list)
    # recom = create_recom(user_list)
    # print("RECOM =>", recom)
    # print("RECOM =>", recom)
    # print("RECOM =>", recom)
    # print("RECOM =>", recom)
    # print("<===END TEST ===>")
    # print("<===END TEST ===>")
    # print("<===END TEST ===>")
    # print("<===END TEST ===>")
    ## END TEST

    # items = {"user_id": items[0]["user_id"],
    #         "articles": items[0]["articles"]
    #         }
    # itema = container.read_item("47681", partition_key="/id")
    # print("ITEMA =>", itema)

    userId = req.params.get('userId')
    if not userId:
        
        try:
            logging.warning("ToDo or items  not found")
        
            req_body = req.get_json()
            print("REQ =>",req.params)
            print("REQBODY =>",req_body)
        except ValueError:
            pass
        else:
            userId = req_body.get('userId')
            print("REQ =>",req.params)
            print("REQBODY =>",req_body)
    if userId:
        # userId = req.params.get('userId')
        # print("USER IDD => ", userId, type(userId))
        # print("USER IDD => ", userId, type(userId))
        # print("USER IDD => ", userId, type(userId))
        # print("USER IDD => ", userId, type(userId))
        # <query_items>
        query = "SELECT * FROM c WHERE c.user_id = @userId",

        userId = int(userId)
        # print("USER CHANGED TYPE => ", userId, type(userId))
        # print("USER CHANGED TYPE => ", userId, type(userId))
        # print("USER CHANGED TYPE => ", userId, type(userId))  
        itemsContainer = createDB("RecommendationDB", "RecommendationContainer", "/user_id")              
        items = list(itemsContainer.query_items(
        query="SELECT * FROM c WHERE c.user_id = @userId",
        parameters=[dict(name="@userId", value=userId)],
        enable_cross_partition_query=True
        ))
        if len(items)<=0:
            userContainer = createDB("UserDB", "UserContainer", "/user_id")              
            user_ = list(userContainer.query_items(
                query="SELECT c.user_id FROM c WHERE c.user_id=@userId",
                parameters=[dict(name="@userId", value=int(userId))],
                enable_cross_partition_query=True
            ))
            if len(user_)<=0:
                return f"User {userId} not found in database"
            else:
                model = load('./model_rec.joblib') 

                pred_df = pd.read_csv("./predict_df.csv")
                user_id_ = 0
                unique_article = pred_df.article_id.unique().tolist()
                recomendations_pred = [(iid ,model.predict(uid=user_id_, iid=str(iid)).est) for iid in unique_article][:5]
                user_recommendations = json.dumps([x[0] for x in recomendations_pred])
                return user_recommendations
        else:
            # print("DB items =>", items[0])
            # print("Found items", items[0])
            # print("TYPE ITEM", type(items[0]))
            item = json.dumps(items[0]["articles"], indent=True)
            # for item in items:
            #     print("DEBUG FOR item ==>", item)

            return item #json.dumps(unique_users, indent=True) #item
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a userId in the query string or in the request body for a personalized response.",
             status_code=200
        )
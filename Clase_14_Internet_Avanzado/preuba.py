from platform import platform
from pandas.core.construction import create_series_with_explicit_dtype
from pymongo.cursor import Cursor
from pymongo.errors import CollectionInvalid
from pymongo.read_preferences import Primary
from conectors.mongodb import remote_server
from pymongo import MongoClient, UpdateOne, UpdateMany
from bson.regex import Regex
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta

if __name__ == "__main__":

        server = remote_server()
        server.start()
        hosts = server.local_bind_port
        created_at= None
        platform_code= None
        created_at_b=None
        platform_code_b=None        
        
        
        with MongoClient(port=hosts, connect=False) as client:
            db = client.data_lake

            query = {"CratedAt": created_at}
            sort = [(u"CreatedAt", -1)]
            projection = {
                "_id": 0.0
            }
            cursor = db['netflixDataDumpLog'].find(query, sort=sort, projection=projection, limit = 1)
            
            platforms=[]
            platform_error=[]
            for item in cursor:
                created_at = item['CreatedAt']
                platforms.append(item)

                collections = ['netflixDataDumpLog']
                for collection in collections:
                    query = {"CreatedAt":created_at,
                        "Error" :  {u"$nin": [u""]} #me trae plataformas con error
                    }
                    projection = {
                        "_id": 0.0
                    }
                    cursor_b= db['netflixDataDumpLog'].find(query, projection=projection)          
                    for item_b in cursor_b:
                        platform_code= item_b["PlatformCode"]
                        platform_error.append(item_b)
                    

            query = {}
            sort = [(u"CREATED_AT", -1)]
            projection = {
                "_id": 0.0
            }
            schema = db['netflixSchemaContents'].find(query, sort=sort, projection=projection)
            
            contents=[]
            for data in schema:
                created_at_b=data["CREATED_AT"]
                platform_code_b=data["PLATFORM_CODE"]
                contents.append(data)

                #if platform_code_b in platform_code:
                    

                        

                        


    


print(platforms)
print(platform_error)
print(platforms_c)




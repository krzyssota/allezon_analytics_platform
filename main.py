from fastapi import FastAPI
from typing import Union
from fastapi import FastAPI, Response
import logging
from classes import UserTag, UserProfile
import threading
import time

from db_client import MyAerospikeClient

app = FastAPI()
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
db_client = MyAerospikeClient()

""""
def thread_function(name):
    logging.info("Thread %s: starting", name)
    time.sleep(2)
    logging.info("Thread %s: finishing", name)


format = "%(asctime)s: %(message)s"
logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")
x = threading.Thread(target=thread_function, args=(1,))
x.start()
x.join()
"""
###        DEBUG ENDPOINTS        ###
@app.get("/ping")
async def ping():
    return Response(status_code=200)

@app.get("/log_all_records")
async def log_all_records():
    db_client.log_all_records()
    return Response(status_code=200)

@app.get("/delete_key/{key}")
async def delete_key(key: str):
    if db_client.delete_key(key):
        code = 200
    else:
        code = 400
    return Response(status_code=code)

@app.get("/log_user_profile/{cookie}")
async def get_user_profile(cookie: str):
    user_profile = db_client.get_user_profile(cookie)
    logger.error(f"User profile {user_profile}")
    return Response(status_code=200)

###    --------------------------    ###

@app.post("/user_tags")
async def user_tags(user_tag: UserTag):
    logger.warning(f"UserTag came {user_tag}")
    db_client.add_tag(user_tag)
    return Response(status_code=204)


@app.post("/user_profiles/{cookie}")
async def user_profiles(cookie: str, time_range: str, user_profile_result: Union[UserProfile, None], limit: int = 200):
    user_profile = db_client.get_user_profile(cookie)
    if user_profile:
        return user_profile
    elif user_profile_result:
        logger.warning(f"no UserProfile", user_profile.cookie)
        return user_profile_result
    else:
        logger.warning(f"no UserProfile", user_profile.cookie)
        return {
            "cookie": cookie,
            "views": [],
            "buys": [],
        }

#if __name__ == "__main__":
#    uvicorn.run(app, host="0.0.0.0", port=8088)
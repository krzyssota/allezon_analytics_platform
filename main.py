from datetime import datetime
from queue import Queue
from typing import Union
from fastapi import FastAPI, Response
import logging
from classes import UserTag, UserProfile
from threading import Thread
from db_client import MyAerospikeClient

WORKER_NUMBER = 4
serve = False
clients = []
queue: Queue
class Worker(Thread):
    queue: Queue
    client: MyAerospikeClient

    def __init__(self, queue: Queue, client: MyAerospikeClient):
        Thread.__init__(self)
        self.queue = queue
        self.client = client

    def run(self):
        global serve
        while serve:
            tag: UserTag = self.queue.get(block=True)
            self.client.add_tag(tag)
            self.queue.task_done()


app = FastAPI()
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
debug_client = MyAerospikeClient()
clients = [MyAerospikeClient() for _ in range(WORKER_NUMBER)]
queue = Queue()
serve = True
for i in range(WORKER_NUMBER):
    w = Worker(queue, clients[i])
    w.daemon = True
    w.start()

@app.on_event("shutdown")
def shutdown():
    global serve
    serve = False
    for c in clients:
        if c:
            c.close()

@app.post("/user_tags")
async def user_tags(user_tag: UserTag):
    global queue
    queue.put(user_tag)
    return Response(status_code=204)


@app.post("/user_profiles/{cookie}")
async def user_profiles(cookie: str, time_range: str, user_profile_result: Union[UserProfile, None] = None, limit: int = 200):
    def filter_tags(tags, time_range):
        time_start = time_range.split("_")[0]
        time_end = time_range.split("_")[1]
        date_format = "%Y-%m-%dT%H:%M:%S.%f"
        ts = datetime.strptime(time_start, date_format)
        te = datetime.strptime(time_end, date_format)
        print("halko ", time_range, "ts ", ts, "te ", te, "type ts", type(ts))
        if len(tags) > 0:
            print("tags[0].time", tags[0].time, "type tag.time", type(tags[0].time))
        logger.error(f"halko {time_range} ts {ts} te {te}")


        return [t for t in tags if ts <= t.time < te]

    user_profile = debug_client.get_user_profile(cookie, -1)
    if user_profile:
        bs = filter_tags(user_profile.buys, time_range)
        vs = filter_tags(user_profile.views, time_range)
        user_profile.views = vs[:limit]
        user_profile.buys = bs[:limit]
        return user_profile
    elif user_profile_result:
        #logger.warning(f"no UserProfile {user_profile.cookie}")
        return user_profile_result
    else:
        logger.warning(f"no UserProfile {user_profile.cookie}")
        return {
            "cookie": cookie,
            "views": [],
            "buys": [],
        }

###        DEBUG ENDPOINTS        ###
@app.get("/ping")
async def ping():
    return Response(status_code=200)

@app.get("/log_all_records")
async def log_all_records():
    debug_client.log_all_records()
    return Response(status_code=200)

@app.get("/delete_key/{key}")
async def delete_key(key: str):
    if debug_client.delete_key(key):
        code = 200
    else:
        code = 400
    return Response(status_code=code)

@app.get("/log_user_profile/{cookie}")
async def get_user_profile(cookie: str):
    user_profile = debug_client.get_user_profile(cookie)
    logger.error(f"User profile {user_profile}")
    return Response(status_code=200)

@app.get("/delete_all_records")
async def delete_all_records():
    debug_client.delete_all_records()
    return Response(status_code=200)

###    --------------------------    ###
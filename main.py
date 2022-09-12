from datetime import datetime
from queue import Queue
from typing import Union
from fastapi import FastAPI, Response
import logging

from pytz import utc

from classes import UserTag, UserProfile
from threading import Thread
from db_client import MyAerospikeClient

from asyncer import asyncify

TAGS_WORKER_NUMBER = 4

class TagsWorker(Thread):
    q: Queue
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





logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
debug_client = MyAerospikeClient()
clients = [MyAerospikeClient() for _ in range(TAGS_WORKER_NUMBER)]
q = Queue()
serve = True
for i in range(TAGS_WORKER_NUMBER):
    w = TagsWorker(q, clients[i])
    w.daemon = True
    w.start()
app = FastAPI()

@app.on_event("shutdown")
def shutdown():
    global serve
    serve = False
    for c in clients:
        if c:
            c.close()


@app.post("/user_tags")
async def user_tags(user_tag: UserTag):
    global q
    q.put(user_tag)
    return Response(status_code=204)


def sync_user_profile(cookie: str, time_range: str, user_profile_result: Union[UserProfile, None] = None,
                        limit: int = 200):
    (user_profile, _) = debug_client.get_user_profile(cookie, -1)
    if user_profile:
        time_start = time_range.split("_")[0]
        time_end = time_range.split("_")[1]
        date_format = "%Y-%m-%dT%H:%M:%S.%f"
        ts = utc.localize(datetime.strptime(time_start, date_format))
        te = utc.localize(datetime.strptime(time_end, date_format))
        bs = [tag for tag in user_profile.buys if ts <= tag.time < te]
        vs = [tag for tag in user_profile.views if ts <= tag.time < te]

        user_profile.views = vs[:limit]
        user_profile.buys = bs[:limit]
        if user_profile.views is None:
            user_profile.views = []
        else:
            user_profile.views.reverse()
        if user_profile.buys is None:
            user_profile.buys = []
        else:
            user_profile.buys.reverse()
        if user_profile_result and user_profile != user_profile_result:
            def as_json(up):
                def objectify(tag):
                    tag.time = str(tag.time)
                    tag.action = tag.action.name
                    tag.device = tag.device.name
                    return vars(tag)

                up.views = list(map(lambda v: objectify(v), up.views))
                up.buys = list(map(lambda b: objectify(b), up.buys))
                return vars(up)

            logger.error(f"diff\nup  {(user_profile)}\nupr {(user_profile_result)}")
        return user_profile
    elif user_profile_result:
        # logger.warning(f"no UserProfile {user_profile.cookie}")
        return user_profile_result
    else:
        logger.warning(f"no UserProfile {cookie}")
        return {
            "cookie": cookie,
            "views": [],
            "buys": [],
        }
@app.post("/user_profiles/{cookie}")
async def user_profiles(cookie: str, time_range: str, user_profile_result: Union[UserProfile, None] = None,
                        limit: int = 200):
    return await asyncify(sync_user_profile)(cookie, time_range, user_profile_result, limit)


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
    (user_profile, _) = debug_client.get_user_profile(cookie, -1)
    logger.error(f"User profile {user_profile}")
    return Response(status_code=200)


@app.get("/delete_all_records")
async def delete_all_records():
    debug_client.delete_all_records()
    return Response(status_code=200)

###    --------------------------    ###

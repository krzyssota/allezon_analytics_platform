from datetime import datetime
from typing import Union
from fastapi import FastAPI, Response
import logging
from pytz import utc
from classes import UserTag, UserProfile
from db_client import MyAerospikeClient

logger = logging.getLogger()
logger.setLevel(logging.ERROR)
db_client = MyAerospikeClient()
app = FastAPI()


@app.on_event("shutdown")
def shutdown():
    db_client.close()


@app.post("/user_tags")
def dummy_user_tags(user_tag: UserTag):
    return Response(status_code=204)


def handle_user_tags(user_tag: UserTag):
    if db_client.add_tag(user_tag):
        return Response(status_code=204)
    else:
        logger.error(f"{user_tag.cookie} couldn't add tag {user_tag}")
        return Response(status_code=400)


@app.post("/user_profiles/{cookie}")
def dummy_user_profiles(cookie: str, time_range: str, user_profile_result: Union[UserProfile, None] = None,
                         limit: int = 200):
    return user_profile_result

def handle_user_profiles(cookie: str, time_range: str, user_profile_result: Union[UserProfile, None] = None,
                         limit: int = 200):
    (user_profile, _) = db_client.get_user_profile(cookie)
    if user_profile:
        # parsing range
        time_start = time_range.split("_")[0]
        time_end = time_range.split("_")[1]
        date_format = "%Y-%m-%dT%H:%M:%S.%f"
        ts = utc.localize(datetime.strptime(time_start, date_format))
        te = utc.localize(datetime.strptime(time_end, date_format))
        # filter
        bs = [tag for tag in user_profile.buys if ts <= tag.time < te]
        vs = [tag for tag in user_profile.views if ts <= tag.time < te]
        user_profile.views = vs[:limit]
        user_profile.buys = bs[:limit]
        if user_profile.views is None:
            user_profile.views = []
        user_profile.views.reverse()
        if user_profile.buys is None:
            user_profile.buys = []
        user_profile.buys.reverse()
        if user_profile_result and user_profile != user_profile_result:
            logger.error(
                f"Difference between my user profile and expected result\nMine: {user_profile}\nExpected: {user_profile_result}")
        return user_profile
    else:  # new user
        return {
            "cookie": cookie,
            "views": [],
            "buys": [],
        }


#          DEVELOPMENT        #
@app.get("/ping")
def ping():
    return Response(status_code=200)


@app.get("/delete_all_records")
def delete_all_records():
    db_client.delete_all_records()
    return Response(status_code=200)

#      ----------------       #

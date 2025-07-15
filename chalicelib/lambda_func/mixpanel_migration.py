import os
import requests

from datetime import datetime

from chalice import Blueprint
from mixpanel import Mixpanel

from chalicelib.db.engine import root_ref
from chalicelib.constants.common import (
    BIRTHDAY,
    DELETED,
    CHALLENGE,
    FLOOR_KEY,
    GENDER_TYPE,
    HEIGHT,
    MAP_KEY,
    JOIN_COUNT,
    NICKNAME,
    PAID,
    PHONE_NUMBER,
    REGISTERED_TIME_UTC,
    TOTAL_CALROIES_BURNED,
    TOTAL_WORKOUT_TIME,
    WEIGHT,
)
from chalicelib.constants.db_ref_key import (
    DB_BETA_USER_DATA,
    DB_DELETED_USER_DATA,
    DB_BETA_USER_EVENT_DATA,
    DB_BETA_USER_FLOOR_DATA,
)
from chalicelib.core import check_subscribing_user, format_utc_timestamp_to_datetime


mixpanel_migration_module = Blueprint(__name__)


PROJECT_TOKEN = os.getenv('MIXPANEL_PROJECT_TOKEN')

BMI = 'BMI'

mp = Mixpanel(PROJECT_TOKEN)

user_data = root_ref.child(DB_BETA_USER_DATA).get()
deleted_user_data = root_ref.child(DB_DELETED_USER_DATA).get()
challenge_data = root_ref.child(DB_BETA_USER_EVENT_DATA).get()
inapp_challenge_info = root_ref.child('inapp_challenge_batch_data').get()
user_floor_data = root_ref.child(DB_BETA_USER_FLOOR_DATA).get()


def flush(payload):
    url = "https://api.mixpanel.com/engage#profile-batch-update"
    headers = {"accept": "text/plain", "content-type": "application/json"}

    response = requests.post(url, json=payload, headers=headers)

    print(response.text)
    return response.text


def reformat_birthdate(date_str):
    if len(date_str) != 6 or not date_str.isdigit():
        return None

    year = int(date_str[:2])
    month = date_str[2:4]
    day = date_str[4:6]

    current_year = datetime.now().year % 100
    if year <= current_year:
        full_year = 2000 + year
    else:
        full_year = 1900 + year

    return f"{full_year}-{month}-{day}"


@mixpanel_migration_module.schedule('cron(5 15 * * ? *)')
def schedule_user_profile_migration(event) -> None:
    sent_count = 0
    payload = []
    active_user_count = 0
    subscribing_user_count = 0
    no_nickname_count = 0

    for user_id, user_info in user_data.items():
        if user_info.get(DELETED):
            user_info = deleted_user_data.get(user_id)
            if not user_info:
                continue
        else:
            active_user_count += 1
            try:
                if check_subscribing_user(user_key=user_id, user_profile=user_info) == PAID:
                    subscribing_user_count += 1
            except Exception as e:
                print(e)
                print(user_id, user_info)
                continue

        user_height = user_info.get(HEIGHT)
        user_weight = user_info.get(WEIGHT)

        user_bmi = (
            round(user_weight / ((user_height / 100) * (user_height / 100)), 1) if user_height and user_weight else 0
        )
        user_nickname = user_info.get(NICKNAME)
        if not user_nickname:
            user_nickname = f'닉네임없음{no_nickname_count}'
            no_nickname_count += 1

        formatted_reg_time = None
        if user_info.get(REGISTERED_TIME_UTC):
            user_registered_time = format_utc_timestamp_to_datetime(user_info.get(REGISTERED_TIME_UTC))
            formatted_reg_time = user_registered_time.strftime("%Y-%m-%dT%H:%M:%S")

        data = {
            '$token': PROJECT_TOKEN,
            '$distinct_id': user_id,
            '$set': {
                '$name': user_nickname,
                HEIGHT: user_height,
                WEIGHT: user_weight,
                NICKNAME: user_nickname,
                BMI: user_bmi,
                PHONE_NUMBER: user_info.get(PHONE_NUMBER),
                JOIN_COUNT: user_info.get(JOIN_COUNT) or 0,
                TOTAL_WORKOUT_TIME: user_info.get(TOTAL_WORKOUT_TIME) or 0,
                TOTAL_CALROIES_BURNED: user_info.get(TOTAL_CALROIES_BURNED) or 0,
                BIRTHDAY: reformat_birthdate(user_info.get(BIRTHDAY, '')),
                GENDER_TYPE: user_info.get(GENDER_TYPE) or None,
                REGISTERED_TIME_UTC: formatted_reg_time if formatted_reg_time else None,
            },
        }

        if user_id in user_floor_data:
            data['$set'][MAP_KEY] = user_floor_data[user_id].get(MAP_KEY)
            data['$set'][FLOOR_KEY] = user_floor_data[user_id].get(FLOOR_KEY) + 1

        challenge_list = []
        for challenge_key, user_list in challenge_data.items():
            challenge_index = inapp_challenge_info[challenge_key]['BatchIndex']
            if user_id in user_list:
                challenge_list.append(f'{challenge_index}기')

        if challenge_list:
            data['$set'][CHALLENGE] = challenge_list

        payload.append(data)

        if len(payload) >= 500:
            flush(payload=payload)
            sent_count += 1
            payload = []
            print(sent_count)

    if payload:
        flush(payload=payload)
        sent_count += 1
        print(sent_count)

    prop = {'ActiveUserCount': active_user_count, 'SubscribingUserCount': subscribing_user_count}
    mp.track(distinct_id='FIVA-Data', event_name='FivaDailyData', properties=prop)

import os
from copy import deepcopy
from datetime import datetime, timedelta, timezone

from chalice import Blueprint, Rate
from chalice import BadRequestError
from chalice.app import Request, Response
from firebase_admin.db import Reference
from mixpanel import Mixpanel

from chalicelib.api_setup import APIHandler, common_set_up
from chalicelib.core import (
    create_change_log_data_set,
    format_utc_timestamp,
    format_utc_timestamp_to_datetime,
    get_active_user_profile,
)
from chalicelib.constants.common import (
    ANIMATION_PLAYED_DOWN,
    ANIMATION_PLAYED_UP,
    COMPLETED_MAPS,
    COMPLETED_TIME_UTC,
    COSTUME_LIST,
    # DEVICES,
    FLOOR_COUNT,
    FLOOR_KEY,
    FLOOR_USER_COUNT,
    FLOORS,
    LAST_ACTIVITY_DATA,
    MAP_KEY,
    NICKNAME,
    ORDER,
    PERCENTAGE,
    PREV_FLOOR,
    # TOKEN,
    UPDATED_TIME_UTC,
    USER_LIST,
    PARTIAL_COMPLETED_TIME_UTC,
)
from chalicelib.constants.db_ref_key import (
    DB_BETA_USER_DATA,
    DB_BETA_USER_DATA_CHANGE_LOG,
    DB_BETA_USER_FLOOR_DATA,
    DB_STAIR_CLIMBING_MAP_DATA,
)
from chalicelib.db.engine import root_ref

# from chalicelib.firebase.core import send_fcm_multicast
from chalicelib.slack_bot import post_slack_message


stair_climbing_api_module = Blueprint(__name__)

TUTORIAL_MAP = 'map0'

MIXPANEL_PROJECT_TOKEN = os.getenv('MIXPANEL_PROJECT_TOKEN')

SCHEDULE_RATE = 1 if os.getenv('SERVER_ENV') == 'prod' else 24


@stair_climbing_api_module.schedule('cron(0 0 * * ? *)')
def schedule_floor_down_alert(event) -> None:
    try:
        user_data = root_ref.child(DB_BETA_USER_DATA).get()
        stair_climbing_map = root_ref.child(DB_STAIR_CLIMBING_MAP_DATA).get()

        max_floor_info = {}
        for map_key, map_info in stair_climbing_map.items():
            max_floor = len(map_info[FLOORS]) - 1
            max_floor_info[map_key] = max_floor

        user_floor_data_ref = root_ref.child(DB_BETA_USER_FLOOR_DATA)
        user_floor_data = user_floor_data_ref.get()

        copied_user_floor_data = deepcopy(user_floor_data)

        for user_key, data in user_floor_data.items():
            updated_time_utc = format_utc_timestamp_to_datetime(data[UPDATED_TIME_UTC])
            last_completed_time = updated_time_utc

            if PARTIAL_COMPLETED_TIME_UTC in data:
                partial_completed_time_utc = format_utc_timestamp_to_datetime(data[PARTIAL_COMPLETED_TIME_UTC])
                if partial_completed_time_utc > updated_time_utc:
                    last_completed_time = partial_completed_time_utc
                    copied_user_floor_data[user_key][UPDATED_TIME_UTC] = format_utc_timestamp(
                        partial_completed_time_utc
                    )

            current_time_utc = datetime.now(timezone.utc)

            data_map_key = data[MAP_KEY]
            data_floor_key = data[FLOOR_KEY]

            if not data_floor_key or data_floor_key == max_floor_info[data_map_key] or data_map_key == TUTORIAL_MAP:
                continue

            if 14 < last_completed_time.hour < 24:
                alert_time_utc = last_completed_time + timedelta(days=3)
            else:
                alert_time_utc = last_completed_time + timedelta(days=2)

            if alert_time_utc < current_time_utc:
                user_profile = get_active_user_profile(user_key, user_data)
                if not user_profile or not isinstance(user_profile.get(DEVICES), dict):
                    continue

                nickname = user_profile.get(NICKNAME, '')
                devices = user_profile.get(DEVICES)

                tokens = [device[TOKEN] for device in devices.values() if device.get(TOKEN)]

                title = f'{nickname}님 안돼...!!'
                body = '내일이면 한 층 떨어져요.\n얼른 들어와서 지금 계단에서 stay 🥹'

                if tokens:
                    send_fcm_multicast(tokens=tokens, title=title, body=body)

    except Exception as e:
        post_slack_message(
            channel_id=os.getenv('SLACK_DEV_CHANNEL_ID'),
            token=os.getenv('SLACK_TOKEN_SERVER'),
            text=f'User Floor Down Alert Failed 🚨\n\nError Message:\n```ERROR: {e}```',
        )
        print(e)


@stair_climbing_api_module.schedule('cron(0 15 * * ? *)')
def schedule_floor_data(event) -> None:
    try:
        mp = Mixpanel(MIXPANEL_PROJECT_TOKEN)

        user_floor_data_ref = root_ref.child(DB_BETA_USER_FLOOR_DATA)
        user_floor_data = user_floor_data_ref.get()
        stair_climbing_map = root_ref.child(DB_STAIR_CLIMBING_MAP_DATA).get()

        max_floor_info = {}
        for map_key, map_info in stair_climbing_map.items():
            max_floor = len(map_info[FLOORS]) - 1
            max_floor_info[map_key] = max_floor

        copied_user_floor_data = deepcopy(user_floor_data)

        for user_key, data in user_floor_data.items():
            updated_time_utc = format_utc_timestamp_to_datetime(data[UPDATED_TIME_UTC])
            last_completed_time = updated_time_utc

            if PARTIAL_COMPLETED_TIME_UTC in data:
                partial_completed_time_utc = format_utc_timestamp_to_datetime(data[PARTIAL_COMPLETED_TIME_UTC])
                if partial_completed_time_utc > updated_time_utc:
                    last_completed_time = partial_completed_time_utc
                    copied_user_floor_data[user_key][UPDATED_TIME_UTC] = format_utc_timestamp(
                        partial_completed_time_utc
                    )

            limit_time_utc = last_completed_time + timedelta(days=3)
            current_time_utc = datetime.now(timezone.utc)

            data_map_key = data[MAP_KEY]
            data_floor_key = data[FLOOR_KEY]

            if not data_floor_key or data_floor_key == max_floor_info[data_map_key] or data_map_key == TUTORIAL_MAP:
                continue

            if current_time_utc > limit_time_utc:
                if LAST_ACTIVITY_DATA not in data:
                    copied_user_floor_data[user_key][LAST_ACTIVITY_DATA] = data

                after_floor_key = data_floor_key - 1

                copied_user_floor_data[user_key][FLOOR_KEY] = after_floor_key
                copied_user_floor_data[user_key][UPDATED_TIME_UTC] = format_utc_timestamp(limit_time_utc)
                copied_user_floor_data[user_key][ANIMATION_PLAYED_DOWN] = False
                copied_user_floor_data[user_key][ANIMATION_PLAYED_UP] = None

                mp.track(
                    distinct_id=user_key,
                    event_name='FloorDown',
                    properties={
                        MAP_KEY: data_map_key,
                        f'Before {FLOOR_KEY}': data_floor_key,
                        f'After {FLOOR_KEY}': after_floor_key,
                    },
                )

        user_floor_data_ref.update(copied_user_floor_data)

    except Exception as e:
        post_slack_message(
            channel_id=os.getenv('SLACK_DEV_CHANNEL_ID'),
            token=os.getenv('SLACK_TOKEN_SERVER'),
            text=f'User Floor Data Update Failed 🚨\n\nError Message:\n```ERROR: {e}```',
        )
        print(e)


@stair_climbing_api_module.schedule(Rate(SCHEDULE_RATE, Rate.HOURS))
def schedule_stair_climbing_data(event) -> None:
    try:
        # Load stair climbing map data
        stair_climbing_map_ref = root_ref.child(DB_STAIR_CLIMBING_MAP_DATA)
        stair_climbing_map = stair_climbing_map_ref.get()

        # mapping default data set
        stair_map_data = {
            map_key: {floor: [] for floor in range(len(floor_info[FLOORS]))}
            for map_key, floor_info in stair_climbing_map.items()
            if map_key != TUTORIAL_MAP
        }

        user_floor_data = root_ref.child(DB_BETA_USER_FLOOR_DATA).get()
        total_climbing_user_count = len(user_floor_data)

        # Collect user floor data and create floor_data_to_update
        for user_key, data in user_floor_data.items():
            map_key = data[MAP_KEY]

            if map_key == TUTORIAL_MAP:
                continue

            if map_key not in stair_map_data:
                stair_map_data[map_key] = {}

            floor = stair_map_data[map_key]
            if data[FLOOR_KEY] not in floor:
                floor[data[FLOOR_KEY]] = [{user_key: data[UPDATED_TIME_UTC]}]
            else:
                floor[data[FLOOR_KEY]].append({user_key: data[UPDATED_TIME_UTC]})

        climbing_user_count = 0
        user_profile_data = root_ref.child(DB_BETA_USER_DATA).get()

        sorted_stair_map_data = dict(
            sorted(stair_map_data.items(), key=lambda x: int(x[0].replace('map', '')), reverse=True)
        )

        # Update stair_climbing_map based on floor_data_to_update
        for map_key, floor_data in sorted_stair_map_data.items():
            sorted_floor_data = dict(sorted(floor_data.items(), reverse=True))
            for floor, user_data_list in sorted_floor_data.items():
                current_floor_data = stair_climbing_map[map_key][FLOORS][floor]

                if not user_data_list:
                    current_floor_data[USER_LIST] = []
                    current_floor_data[PERCENTAGE] = 0
                    current_floor_data[FLOOR_USER_COUNT] = 0
                else:
                    user_data_list.sort(
                        key=lambda x: format_utc_timestamp_to_datetime(list(x.values())[0]), reverse=True
                    )
                    floor_user_count = len(user_data_list)
                    current_floor_data[FLOOR_USER_COUNT] = floor_user_count

                    percentage = (climbing_user_count + 1) / total_climbing_user_count

                    current_floor_data[PERCENTAGE] = percentage

                    climbing_user_count += floor_user_count

                    recent_user_data = {}
                    for index, recent_user in enumerate(user_data_list):
                        if len(recent_user_data) >= 4:
                            break
                        for user_key in recent_user:
                            user_profile = get_active_user_profile(user_key, user_profile_data)

                            if user_profile:
                                recent_user_data[user_key] = {
                                    NICKNAME: user_profile.get(NICKNAME),
                                    COSTUME_LIST: user_profile.get(COSTUME_LIST),
                                    ORDER: index,
                                }
                    current_floor_data[USER_LIST] = recent_user_data

        stair_climbing_map_ref.update(stair_climbing_map)

    except Exception as e:
        post_slack_message(
            channel_id=os.getenv('SLACK_DEV_CHANNEL_ID'),
            token=os.getenv('SLACK_TOKEN_SERVER'),
            text=f'Climbing Stair Data Update Failed 🚨\n\nError Message:\n```ERROR: {e}```',
        )
        print(e)


@stair_climbing_api_module.route('/stair', methods=['POST'])
@common_set_up(module=stair_climbing_api_module)
def climbing_stair_api(request: Request, root_ref: Reference, handler: APIHandler) -> Response:
    user_id = request.query_params.get('UserId')
    if not user_id:
        raise BadRequestError('Missing user ID in the request')

    stair_floor_info = request.json_body
    if not stair_floor_info:
        raise BadRequestError('Missing body in the request')

    stair_climbing_map_ref = root_ref.child(DB_STAIR_CLIMBING_MAP_DATA)
    stair_climbing_map = stair_climbing_map_ref.get()

    map_key = stair_floor_info[MAP_KEY]
    if map_key not in stair_climbing_map:
        raise BadRequestError('Invalid map key')

    floor_key = stair_floor_info[FLOOR_KEY]
    if floor_key > len(stair_climbing_map[map_key][FLOORS]):
        raise BadRequestError('Invalid floor key')

    user_floor_data_ref = root_ref.child(DB_BETA_USER_FLOOR_DATA).child(user_id)
    user_floor_data = user_floor_data_ref.get() or {FLOOR_KEY: 0, MAP_KEY: TUTORIAL_MAP}

    current_user_floor_data = {FLOOR_KEY: user_floor_data[FLOOR_KEY], MAP_KEY: user_floor_data[MAP_KEY]}
    prev_floor_data = (
        {FLOOR_KEY: floor_key - 1, MAP_KEY: map_key} if floor_key else stair_climbing_map[map_key][PREV_FLOOR]
    )

    if current_user_floor_data != prev_floor_data:
        raise BadRequestError('Previous floor data mismatch')

    next_floor_info = {
        MAP_KEY: map_key,
        FLOOR_KEY: floor_key,
        ANIMATION_PLAYED_UP: False,
        UPDATED_TIME_UTC: handler.timestamp,
        LAST_ACTIVITY_DATA: None,
        ANIMATION_PLAYED_DOWN: None,
    }

    if floor_key == stair_climbing_map[map_key][FLOOR_COUNT] - 1:
        next_floor_info[COMPLETED_MAPS] = {
            map_key: {
                user_floor_data_ref.child(COMPLETED_MAPS)
                .child(map_key)
                .push()
                .key: {COMPLETED_TIME_UTC: handler.timestamp}
            }
        }

    # Update user floor data
    user_floor_data_ref.update(next_floor_info)

    # Update user data change log
    root_ref.child(DB_BETA_USER_DATA_CHANGE_LOG).child(user_id).push().set(
        create_change_log_data_set(ref_key=FLOORS, new_data=next_floor_info)
    )

    return handler.response('', 201)

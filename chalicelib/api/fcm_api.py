import json
import random

from datetime import datetime, timedelta, timezone
from typing import Any

from chalice import BadRequestError
from chalice import Blueprint
from chalice.app import Request, Response
from firebase_admin.db import Reference

from chalicelib.api_setup import APIHandler, common_set_up
from chalicelib.constants.common import (
    ACTIVITY,
    BODY,
    CONTENT_INFO,
    CONTENT_KEY,
    CONTENT_TYPE,
    DATA,
    DATETIME_KEY,
    DURATION_SEC,
    FCM_METHOD,
    FILTER_TYPES,
    KCAL,
    LIVE_TITLE,
    PUSH_KEY,
    PUSH_NOTIFICATION_OPENED,
    TITLE,
    TYPE,
    USER_DEVICES,
    USER_KEY,
    VALUE,
)
from chalicelib.constants.db_ref_key import DB_CONTENT_INFO, DB_LIVE_SCHEDULE_INFO
from chalicelib.core import create_activity_after_24_notification_schedule, format_unix_timestamp


fcm_api_module = Blueprint(__name__)


class FcmAPIHandler:
    def __init__(self, root_ref: Reference, user_id: str, body: dict = {}):
        self.root_ref = root_ref
        self.user_id = user_id
        self.default_fcm_title = '안녕! FIVA에요'

        self.activity_fcm_func_mapping = {
            'DefaultWorkout': lambda: self._schedule_default_workout_remind(body),
            'SkiGame': self._schedule_ski_game_remind,
            'ArmFlightGame': self._schedule_arm_flight_game_remind,
        }

    def _schedule_default_workout_remind(self, body: dict) -> Any:
        content_type = body[CONTENT_INFO][CONTENT_TYPE]
        content_key = body[CONTENT_INFO][CONTENT_KEY]

        content = self.root_ref.child(DB_CONTENT_INFO).child(content_type).child(content_key).get()
        if not content:
            raise BadRequestError('Invalid content')

        content_title = content.get(f'{TITLE}Text', '')
        content_info = {
            CONTENT_TYPE: content_type,
            CONTENT_KEY: content_key,
            DURATION_SEC: content[DURATION_SEC],
            KCAL: content[KCAL],
            DATETIME_KEY: content['Date'],
            FILTER_TYPES: content.get(FILTER_TYPES, []),
            f'{TITLE}Text': content_title,
        }

        nickname = '{Nickname}'
        fcm_body = None
        fcm_data = {}
        if content_type == 'LiveList':
            date_format = '%Y-%m-%d %I:%M%p'
            date_obj = datetime.strptime(content_info[DATETIME_KEY], date_format)
            class_hour = date_obj.hour

            live_index = 0
            if class_hour == 21:
                live_index = 1
            elif class_hour == 22:
                live_index = 2

            tomorrow = date_obj + timedelta(days=1)
            tomorrow_month_key = tomorrow.strftime('%Y-%m')
            tomorrow_datetime_key = tomorrow.strftime(date_format)
            live_info = (
                self.root_ref.child(DB_LIVE_SCHEDULE_INFO)
                .child(f'live_class_{live_index}')
                .child(tomorrow_month_key)
                .child(tomorrow_datetime_key)
                .get()
            )
            live_title = live_info[LIVE_TITLE]
            live_content_key = live_info[CONTENT_KEY]
            fcm_data = {
                PUSH_NOTIFICATION_OPENED: json.dumps(
                    {
                        PUSH_KEY: 'ActRemind24',
                        ACTIVITY: 'DefaultWorkout',
                        VALUE: {
                            CONTENT_TYPE: content_type,
                            CONTENT_KEY: content_key,
                            TITLE: content_title,
                            'TargetLiveContentKey': live_content_key,
                            'SendTimeUtc': format_unix_timestamp(),
                        },
                    }
                )
            }

            fcm_body = f'{nickname}님! 오늘은 "{live_title}" 클래스 어때요?'

        if content_type == 'VodList':
            target_text_map = {'FULL': '전신', 'UPPER': '상체', 'LOWER': '하체', 'CORE': '코어'}
            target_text_list = ['FULL', 'UPPER', 'LOWER', 'CORE']

            target_text_list = [
                target_text for target_text in target_text_list if target_text not in content_info[FILTER_TYPES]
            ]

            random_body_parts = target_text_map[random.choice(target_text_list)]
            fcm_data = {
                PUSH_NOTIFICATION_OPENED: json.dumps(
                    {
                        PUSH_KEY: 'ActRemind24',
                        ACTIVITY: 'DefaultWorkout',
                        VALUE: {
                            CONTENT_TYPE: content_type,
                            CONTENT_KEY: content_key,
                            TITLE: content_title,
                            'TargetRandomBodyParts': random_body_parts,
                            'SendTimeUtc': format_unix_timestamp(),
                        },
                    }
                )
            }

            fcm_body = f'{nickname}님! 어제 "{content_title}"은 어떠셨나요?\n\
오늘은 {random_body_parts}운동 어때요?'

        if fcm_body:
            return create_activity_after_24_notification_schedule(
                user_id=self.user_id,
                payload={
                    FCM_METHOD: USER_DEVICES,
                    USER_KEY: self.user_id,
                    TITLE: self.default_fcm_title,
                    BODY: fcm_body,
                    DATA: fcm_data,
                },
            )

    def _schedule_ski_game_remind(self):
        fcm_body = '{Nickname}님! 오늘도 스키게임으로 허벅지와 엉덩이 모두 불태워보자구요!'
        fcm_data = {
            PUSH_NOTIFICATION_OPENED: json.dumps(
                {
                    PUSH_KEY: 'ActRemind24',
                    ACTIVITY: 'MotionGame',
                    VALUE: {TYPE: 'SkiGame', 'SendTimeUtc': format_unix_timestamp()},
                }
            )
        }
        return create_activity_after_24_notification_schedule(
            user_id=self.user_id,
            payload={
                FCM_METHOD: USER_DEVICES,
                USER_KEY: self.user_id,
                TITLE: self.default_fcm_title,
                BODY: fcm_body,
                DATA: fcm_data,
            },
        )

    def _schedule_arm_flight_game_remind(self):
        fcm_body = '{Nickname}님! 아직 팔에 덜렁이 살이 고민인가요? 오늘도 비행게임으로 탄탄하게 만들어요!'
        fcm_data = {
            PUSH_NOTIFICATION_OPENED: json.dumps(
                {
                    PUSH_KEY: 'ActRemind24',
                    ACTIVITY: 'MotionGame',
                    VALUE: {TYPE: 'ArmFlightGame', 'SendTimeUtc': format_unix_timestamp()},
                }
            )
        }
        return create_activity_after_24_notification_schedule(
            user_id=self.user_id,
            payload={
                FCM_METHOD: USER_DEVICES,
                USER_KEY: self.user_id,
                TITLE: self.default_fcm_title,
                BODY: fcm_body,
                DATA: fcm_data,
            },
        )


@fcm_api_module.route('/fcm/act-remind', methods=['POST'])
@common_set_up(module=fcm_api_module)
def workout_log_init_api(request: Request, root_ref: Reference, handler: APIHandler) -> Response:
    user_id = request.query_params.get('UserId')
    if not user_id:
        raise BadRequestError('Missing user ID in the request')

    body = request.json_body
    if not body:
        raise BadRequestError('Missing body in the request')

    activity = body.pop(ACTIVITY, None)
    if not activity:
        raise BadRequestError('Missing activity in the request')

    fcm_api_handler = FcmAPIHandler(root_ref=root_ref, user_id=user_id, body=body)
    remind_func = fcm_api_handler.activity_fcm_func_mapping.get(activity)
    if remind_func:
        remind_func()

    return handler.response('', 201)

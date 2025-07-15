import os
import random

from datetime import datetime, timedelta, timezone
from typing import Any

from chalice import BadRequestError
from chalice import Blueprint, CORSConfig
from chalice.app import Request, Response
from firebase_admin.db import Reference

from chalicelib.api_setup import APIHandler, common_set_up
from chalicelib.constants.common import (
    CALCULATED_LOGS,
    CONTENT_INFO,
    CONTENT_KEY,
    CONTENT_TYPE,
    DATETIME_KEY,
    DURATION_SEC,
    EVENT_TIME_UTC,
    FILTER_TYPES,
    JOIN_COUNT,
    KCAL,
    LOGS,
    JOIN_COUNT_STATUS,
    TITLE,
    TOTAL_CALROIES_BURNED,
    TOTAL_WORKOUT_TIME,
)
from chalicelib.constants.db_ref_key import (
    DB_BETA_USER_DATA,
    DB_BETA_USER_DATA_CHANGE_LOG,
    DB_BETA_USER_WORKOUT_LOGS,
    DB_CONTENT_INFO,
    DB_LIVE_SCHEDULE_INFO,
    DB_WORKOUT_RECORD_CHANGES_DATE_GROUPED,
    DB_WORKOUT_RECORD_CHANGES_USER_DATE_GROUPED,
)
from chalicelib.core import create_change_log_data_set, format_kst_date_str, format_utc_date_str


workout_logs_api_module = Blueprint(__name__)

cors_config = CORSConfig(allow_origin=os.getenv('BASE_URL'))


class WorkoutLogHandler:
    def __init__(self, root_ref: Reference, user_id: str, body: dict[str, Any], timestamp: str):
        self.root_ref = root_ref
        self.user_id = user_id
        self.body = body
        self.timestamp = timestamp

        self.user_workout_logs_ref = root_ref.child(DB_BETA_USER_WORKOUT_LOGS).child(user_id).child(body[DATETIME_KEY])
        self.user_workout_data_info = self.user_workout_logs_ref.get()

        if self.body.get('LogInfo'):
            self.body['LogInfo'][EVENT_TIME_UTC] = timestamp

    def get_content_info(self) -> dict:
        content_type = self.body[CONTENT_INFO][CONTENT_TYPE]
        content_key = self.body[CONTENT_INFO][CONTENT_KEY]

        content = self.root_ref.child(DB_CONTENT_INFO).child(content_type).child(content_key).get()
        content_info = {
            CONTENT_TYPE: content_type,
            CONTENT_KEY: content_key,
            DURATION_SEC: content[DURATION_SEC],
            KCAL: content[KCAL],
            DATETIME_KEY: content['Date'],
            FILTER_TYPES: content.get(FILTER_TYPES, []),
            f'{TITLE}Text': content.get(f'{TITLE}Text', ''),
        }

        return content_info

    def initialize_workout_logs(self, content_info: dict) -> bool:
        if self.user_workout_data_info:
            return False

        self.root_ref.child(DB_BETA_USER_WORKOUT_LOGS).child(self.user_id).update(
            {
                self.body[DATETIME_KEY]: {
                    CONTENT_INFO: content_info,
                    EVENT_TIME_UTC: self.timestamp,
                    JOIN_COUNT_STATUS: False,
                }
            }
        )
        self.user_workout_data_info = self.user_workout_logs_ref.get()
        return True

    def set_log(self) -> None:
        self.user_workout_logs_ref.child(LOGS).push().set(self.body['LogInfo'])

    def calculate_logs(self) -> dict[str, Any]:
        workout_logs = self.user_workout_data_info.get(LOGS, {})
        workout_logs[self.user_workout_logs_ref.child(CALCULATED_LOGS).push().key] = self.body['LogInfo']

        content_duration = self.user_workout_data_info[CONTENT_INFO][DURATION_SEC]
        content_calories = self.user_workout_data_info[CONTENT_INFO][KCAL]

        calculated_duration = sum(
            float(value[DURATION_SEC])
            for value in self.user_workout_data_info.get(CALCULATED_LOGS, {}).values()
            if value
        )

        workout_duration = sum(float(value[DURATION_SEC]) for value in workout_logs.values() if value)
        workout_calories = round((content_calories / content_duration) * workout_duration, 1)
        workout_succeeded = (calculated_duration + workout_duration) >= 0.7 * content_duration

        self.user_workout_logs_ref.child(LOGS).delete()
        self.user_workout_logs_ref.child(CALCULATED_LOGS).update(workout_logs)

        return {
            DURATION_SEC: workout_duration,
            KCAL: workout_calories,
            JOIN_COUNT_STATUS: self.user_workout_data_info[JOIN_COUNT_STATUS],
            'workout_succeeded': workout_succeeded,
            DATETIME_KEY: self.body[DATETIME_KEY],
            EVENT_TIME_UTC: self.timestamp,
        }

    def update_user_workout_data(self, calculated_logs: dict[str, Any]) -> None:
        user_ref = self.root_ref.child(DB_BETA_USER_DATA).child(self.user_id)
        user_data_change_log_ref = self.root_ref.child(DB_BETA_USER_DATA_CHANGE_LOG).child(self.user_id)
        workout_record_changes_date_grouped_ref = (
            self.root_ref.child(DB_WORKOUT_RECORD_CHANGES_DATE_GROUPED).child(format_utc_date_str()).child(self.user_id)
        )

        workout_record_changes_user_date_grouped_ref = (
            self.root_ref.child(DB_WORKOUT_RECORD_CHANGES_USER_DATE_GROUPED)
            .child(self.user_id)
            .child(format_kst_date_str())
        )

        user_data_info = user_ref.get()
        old_user_data = {
            TOTAL_WORKOUT_TIME: user_data_info.get(TOTAL_WORKOUT_TIME) or 0,
            TOTAL_CALROIES_BURNED: user_data_info.get(TOTAL_CALROIES_BURNED) or 0,
        }

        new_user_data = {
            TOTAL_WORKOUT_TIME: round(old_user_data[TOTAL_WORKOUT_TIME] + (calculated_logs[DURATION_SEC] / 60), 1),
            TOTAL_CALROIES_BURNED: round(old_user_data[TOTAL_CALROIES_BURNED] + calculated_logs[KCAL], 1),
        }

        if not calculated_logs[JOIN_COUNT_STATUS] and calculated_logs['workout_succeeded']:
            old_user_data[JOIN_COUNT] = user_data_info.get(JOIN_COUNT) or 0
            new_user_data[JOIN_COUNT] = old_user_data[JOIN_COUNT] + 1
            self.user_workout_logs_ref.update({JOIN_COUNT_STATUS: True})

        user_ref.update(new_user_data)

        user_data_change_log_ref.push().set(
            create_change_log_data_set(ref_key='WorkoutRecord', new_data=new_user_data, old_data=old_user_data)
        )

        workout_record_changes_date_grouped_ref.push().set(calculated_logs)
        workout_record_changes_user_date_grouped_ref.push().set(calculated_logs)


@workout_logs_api_module.route('/workout-logs/init', methods=['POST'])
@common_set_up(module=workout_logs_api_module)
def workout_log_init_api(request: Request, root_ref: Reference, handler: APIHandler) -> Response:
    user_id = request.query_params.get('UserId')
    if not user_id:
        raise BadRequestError('Missing user ID in the request')

    body = request.json_body
    if not body:
        raise BadRequestError('Missing body in the request')

    workout_log_handler = WorkoutLogHandler(root_ref=root_ref, user_id=user_id, body=body, timestamp=handler.timestamp)
    content_info = workout_log_handler.get_content_info()

    initialization_res = workout_log_handler.initialize_workout_logs(content_info)

    if not initialization_res:
        return handler.response({'Initialized': True}, 200)

    return handler.response('', 201)


@workout_logs_api_module.route('/workout-logs', methods=['POST'], cors=cors_config)
@common_set_up(module=workout_logs_api_module)
def workout_log_api(request: Request, root_ref: Reference, handler: APIHandler) -> Response:
    user_id = request.query_params.get('UserId')
    if not user_id:
        raise BadRequestError('Missing user ID in the request')

    body = request.json_body
    if not body:
        raise BadRequestError('Missing body in the request')

    workout_log_handler = WorkoutLogHandler(root_ref=root_ref, user_id=user_id, body=body, timestamp=handler.timestamp)
    content_info = workout_log_handler.get_content_info()

    workout_log_handler.initialize_workout_logs(content_info)

    log_datetime = datetime.strptime(body[DATETIME_KEY], '%Y-%m-%dT%I_%M_%S_%f%p').replace(tzinfo=timezone.utc)
    if log_datetime > datetime.now(timezone.utc):
        return handler.response('', 201)

    if body['LogInfo']['EventType'] == 'InProgress':
        workout_log_handler.set_log()
        return handler.response('', 201)

    calculated_logs = workout_log_handler.calculate_logs()
    workout_log_handler.update_user_workout_data(calculated_logs)

    return handler.response('', 201)


@workout_logs_api_module.route('/workout-logs/pending-process', methods=['POST'])
@common_set_up(module=workout_logs_api_module)
def workout_log_abnormal_check_api(request: Request, root_ref: Reference, handler: APIHandler) -> Response:
    user_id = request.query_params.get('UserId')
    if not user_id:
        raise BadRequestError('Missing user ID in the request')

    user_workout_logs = root_ref.child(DB_BETA_USER_WORKOUT_LOGS).child(user_id).get()
    if not user_workout_logs:
        return handler.response('', 201)

    for ref_key, log_info in user_workout_logs.items():
        if log_info.get(LOGS):
            body = {DATETIME_KEY: ref_key, 'LogInfo': {'EventType': 'AutoClosed', DURATION_SEC: 0}}
            workout_log_handler = WorkoutLogHandler(
                root_ref=root_ref, user_id=user_id, body=body, timestamp=handler.timestamp
            )
            calculated_logs = workout_log_handler.calculate_logs()
            workout_log_handler.update_user_workout_data(calculated_logs)

    return handler.response('', 201)

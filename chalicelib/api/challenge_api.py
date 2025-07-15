from datetime import datetime, timezone
from typing import Any

from chalice import Blueprint
from chalice import BadRequestError
from chalice.app import Request, Response
from firebase_admin.db import Reference

from chalicelib.api.activity_coin_api import ActivityCoinAPIHandler
from chalicelib.api_setup import APIHandler, common_set_up
from chalicelib.constants.common import (
    ACTION,
    ACTIVITY,
    DURATION_SEC,
    END_TIME_UTC,
    EVENT_TIME_UTC,
    OBJECTIVE_TYPE,
    START_TIME_UTC,
    TYPE,
    VALUE,
)
from chalicelib.constants.db_ref_key import (
    DB_BETA_USER_CHALLENGE_MISSION_COMPLETED_DATA,
    DB_BETA_USER_DATA_CHANGE_LOG,
    DB_BETA_USER_EVENT_DATA,
    DB_BETA_USER_CHALLENGE_SUCCEEDED_DATA,
    DB_BETA_USER_ITEM_DATA,
    DB_BETA_USER_REWARD_POPUP,
    DB_INAPP_CHALLENGE_MISSION_DATA,
    DB_WORKOUT_RECORD_CHANGES_USER_DATE_GROUPED,
    DB_INAPP_CHALLENGE_BATCH_DATA,
)
from chalicelib.core import (
    async_fetch_paths,
    create_change_log_data_set,
    format_kst_timestamp_to_datetime,
    format_utc_timestamp,
    format_utc_timestamp_to_datetime,
)


challenge_api_module = Blueprint(__name__)


class ChallengeHandler:
    def __init__(self, user_id: str):
        self.user_id = user_id

    def check_user_challenge_participation(self, user_event_data: dict, challenge_key: str) -> bool:
        user_list = user_event_data.get(challenge_key)
        if user_list and isinstance(user_list, list) and self.user_id in user_list:
            return True
        return False

    def check_user_challenge_succeeded(self, user_challenge_succeeded_data: dict, challenge_key: str) -> bool:
        user_list = user_challenge_succeeded_data.get(challenge_key)
        if user_list and isinstance(user_list, list) and self.user_id in user_list:
            return True
        return False


class ChallengeMissionHandler(ChallengeHandler):
    def __init__(self, user_id: str, root_ref: Reference, activity_type: str, sub_type: str, action: dict) -> Any:
        super().__init__(user_id)
        self.now_utc = datetime.now().replace(tzinfo=timezone.utc)

        self.action_map = {'TargetContent': 'ContentKey', 'PlayCount': 'Count', 'PlayScore': 'Score'}

        self.user_id = user_id
        self.root_ref = root_ref
        self.activity_type = activity_type
        self.sub_type = sub_type
        self.action = action

        self._validate_action()

    def _validate_action(self) -> None:
        objective_type = self.action.get(OBJECTIVE_TYPE)
        if (
            not objective_type
            or objective_type not in self.action_map
            or self.action_map[objective_type] not in self.action[VALUE]
        ):
            raise BadRequestError('Invalid action')

    def get_current_challenge_key_list(self, inapp_challenge_data: dict) -> list:
        current_challenge_keys = [
            key
            for key, data in inapp_challenge_data.items()
            if format_utc_timestamp_to_datetime(data[START_TIME_UTC])
            <= self.now_utc
            < format_utc_timestamp_to_datetime(data[END_TIME_UTC])
        ]
        return current_challenge_keys

    def get_requested_in_progress_mission(self, challenge_missions: dict) -> Any:
        for mission_key, mission_info in challenge_missions.items():
            if (
                self.activity_type == mission_info[f'{ACTIVITY}{TYPE}']
                and self.sub_type == mission_info[f'Sub{TYPE}']
                and self.action[OBJECTIVE_TYPE] == mission_info[ACTION][OBJECTIVE_TYPE]
                and format_utc_timestamp_to_datetime(mission_info[START_TIME_UTC])
                <= self.now_utc
                < format_utc_timestamp_to_datetime(mission_info[END_TIME_UTC])
            ):
                return mission_key, mission_info
        return None, {}

    def is_mission_successful(self, mission_info: dict) -> bool:
        objective_type = self.action[OBJECTIVE_TYPE]
        key = self.action_map[objective_type]

        if self.action[OBJECTIVE_TYPE] == 'TargetContent':
            return self.action[VALUE][key] in mission_info[ACTION][VALUE][f'{key}s']
        else:
            return self.action[VALUE][key] >= mission_info[ACTION][VALUE][key]

    def reward_user_for_successful_mission(self, mission_info: dict) -> None:
        for reward in mission_info.get('Rewards', []):
            if reward[TYPE] == 'Coin':
                ActivityCoinAPIHandler(
                    root_ref=self.root_ref, user_id=self.user_id, activity='ChallengeMission'
                ).update_user_activity_coins(reward[VALUE])

            if reward[TYPE] == 'Item':
                self.root_ref.child(DB_BETA_USER_ITEM_DATA).child(self.user_id).update(
                    {reward[VALUE]: format_utc_timestamp()}
                )
                self.root_ref.child(DB_BETA_USER_DATA_CHANGE_LOG).child(self.user_id).push().set(
                    create_change_log_data_set(ref_key='RewardItem', new_data=[reward[VALUE]])
                )

    def update_user_challenge_mission_data(self, challenge_key: str, mission_key: str, mission_info: dict) -> None:
        updates = {
            f'{DB_BETA_USER_CHALLENGE_MISSION_COMPLETED_DATA}/{self.user_id}/{challenge_key}/{mission_key}': {
                EVENT_TIME_UTC: format_utc_timestamp()
            },
            f'{DB_BETA_USER_REWARD_POPUP}/{self.user_id}/{challenge_key}_{mission_key}': mission_info['Popup'],
        }
        self.root_ref.update(updates)


@challenge_api_module.route('/challenge/overall-status', methods=['GET'])
@common_set_up(module=challenge_api_module)
def challenge_overall_status_api(request: Request, root_ref: Reference, handler: APIHandler) -> Response:
    user_id = request.query_params.get('UserId')
    if not user_id:
        raise BadRequestError('Missing user ID in the request')
    year = request.query_params.get('Year')
    now = datetime.now()
    if not year:
        year = str(now.year)
    else:
        year = str(year)

    path_list = [DB_BETA_USER_EVENT_DATA, DB_BETA_USER_CHALLENGE_SUCCEEDED_DATA, DB_INAPP_CHALLENGE_BATCH_DATA]
    data = async_fetch_paths(root_ref, path_list)

    user_event_data = data.get(DB_BETA_USER_EVENT_DATA, {})
    inapp_challenge_data = data.get(DB_INAPP_CHALLENGE_BATCH_DATA, {})
    user_challenge_succeeded_data = data.get(DB_BETA_USER_CHALLENGE_SUCCEEDED_DATA, {})  # User List

    challenge_handler = ChallengeHandler(user_id=user_id)

    result = {}
    for challenge_key, challenge_data in inapp_challenge_data.items():
        if year in challenge_key:
            start_time_str = challenge_data[START_TIME_UTC]
            end_time_str = challenge_data[END_TIME_UTC]

            start_time = format_kst_timestamp_to_datetime(start_time_str)
            end_time = format_kst_timestamp_to_datetime(end_time_str)

            application_start_time = format_kst_timestamp_to_datetime(challenge_data['ApplicationStartTimeUtc'])
            application_due_time = format_kst_timestamp_to_datetime(challenge_data['ApplicationDueTimeUtc'])

            if start_time > now:
                challenge_status = 'Applying' if application_start_time <= now < application_due_time else 'NotStarted'
            elif end_time < now:
                if not challenge_handler.check_user_challenge_participation(
                    user_event_data=user_event_data, challenge_key=challenge_key
                ):
                    challenge_status = 'NotParticipated'
                else:
                    challenge_status = (
                        'Succeed'
                        if challenge_handler.check_user_challenge_succeeded(
                            user_challenge_succeeded_data=user_challenge_succeeded_data, challenge_key=challenge_key
                        )
                        else 'Failed'
                    )
            elif start_time <= now < end_time:
                challenge_status = (
                    'Participating'
                    if challenge_handler.check_user_challenge_participation(
                        user_event_data=user_event_data, challenge_key=challenge_key
                    )
                    else 'NotParticipated'
                )
            result[challenge_key] = {
                'Status': challenge_status,
                'Month': challenge_data.get('Month'),
                'StartDate': start_time_str,
                'EndDate': end_time_str,
            }

    return handler.response(result, 200)


@challenge_api_module.route('/challenge/user-record', methods=['POST'])
@common_set_up(module=challenge_api_module)
def challenge_user_record_api(request: Request, root_ref: Reference, handler: APIHandler) -> Response:
    user_id = request.query_params.get('UserId')
    if not user_id:
        raise BadRequestError('Missing user ID in the request')

    body = request.json_body
    if not body:
        raise BadRequestError('Missing body in the request')

    challenge_batch_list = body.get('BatchKeys')
    if not challenge_batch_list:
        raise BadRequestError('Missing challenge batch keys in the request')

    path_list = [f'{DB_WORKOUT_RECORD_CHANGES_USER_DATE_GROUPED}/{user_id}', DB_INAPP_CHALLENGE_BATCH_DATA]
    data = async_fetch_paths(root_ref, path_list)

    user_workout_record_changes_date_grouped = data.get(f'{DB_WORKOUT_RECORD_CHANGES_USER_DATE_GROUPED}/{user_id}', {})
    inapp_challenge_data = data.get(DB_INAPP_CHALLENGE_BATCH_DATA, {})

    result = {}
    for challenge_batch_key in challenge_batch_list:
        records = {'Records': {}}

        if not inapp_challenge_data.get(challenge_batch_key):
            continue

        challenge_data = inapp_challenge_data[challenge_batch_key]
        records['Threshold'] = challenge_data.get('Threshold', 1200)
        if not user_workout_record_changes_date_grouped:
            result[challenge_batch_key] = records
            continue

        start_time_str = challenge_data[START_TIME_UTC]
        end_time_str = challenge_data[END_TIME_UTC]

        start_time = format_kst_timestamp_to_datetime(start_time_str)
        end_time = format_kst_timestamp_to_datetime(end_time_str)

        start_date = datetime(start_time.year, start_time.month, start_time.day)
        end_date = datetime(end_time.year, end_time.month, end_time.day)

        for date_key, workout_data in user_workout_record_changes_date_grouped.items():
            workout_date = datetime.strptime(date_key, '%Y-%m-%d')
            if start_date <= workout_date < end_date:
                total_duration = sum(value[DURATION_SEC] for value in workout_data.values())
                records['Records'][date_key] = {DURATION_SEC: total_duration}

        result[challenge_batch_key] = records

    return handler.response(result, 200)


@challenge_api_module.route('/challenge/mission', methods=['POST'])
@common_set_up(module=challenge_api_module)
def challenge_mission_api(request: Request, root_ref: Reference, handler: APIHandler) -> Response:
    user_id = request.query_params.get('UserId')
    if not user_id:
        raise BadRequestError('Missing user ID in the request')

    body = request.json_body
    if not body:
        raise BadRequestError('Missing body in the request')

    activity_type = body.get(f'{ACTIVITY}{TYPE}')
    sub_type = body.get(f'Sub{TYPE}')
    action = body.get(ACTION)

    if not activity_type or not sub_type or not isinstance(action, dict):
        raise BadRequestError('Invalid activity')

    path_list = [
        f'{DB_BETA_USER_CHALLENGE_MISSION_COMPLETED_DATA}/{user_id}',
        DB_BETA_USER_EVENT_DATA,
        DB_INAPP_CHALLENGE_BATCH_DATA,
        DB_INAPP_CHALLENGE_MISSION_DATA,
    ]
    data = async_fetch_paths(root_ref, path_list)

    user_event_data = data.get(DB_BETA_USER_EVENT_DATA, {})
    user_mission_data = data.get(f'{DB_BETA_USER_CHALLENGE_MISSION_COMPLETED_DATA}/{user_id}', {})
    inapp_challenge_data = data.get(DB_INAPP_CHALLENGE_BATCH_DATA, {})
    inapp_challenge_mission_data = data.get(DB_INAPP_CHALLENGE_MISSION_DATA, {})

    challenge_mission_handler = ChallengeMissionHandler(
        user_id=user_id, root_ref=root_ref, activity_type=activity_type, sub_type=sub_type, action=action
    )

    current_challenge_keys = challenge_mission_handler.get_current_challenge_key_list(
        inapp_challenge_data=inapp_challenge_data
    )
    if not current_challenge_keys:
        raise BadRequestError('There are no ongoing challenges.')

    for challenge_key in current_challenge_keys:
        if not challenge_mission_handler.check_user_challenge_participation(
            user_event_data=user_event_data, challenge_key=challenge_key
        ):
            continue
        if challenge_key not in inapp_challenge_mission_data:
            continue

        challenge_missions = inapp_challenge_mission_data[challenge_key]
        mission_key, mission_info = challenge_mission_handler.get_requested_in_progress_mission(challenge_missions)
        if (
            not mission_key
            or not mission_info
            or mission_key in user_mission_data.get(challenge_key, {})
            or not challenge_mission_handler.is_mission_successful(mission_info)
        ):
            continue

        challenge_mission_handler.reward_user_for_successful_mission(mission_info=mission_info)
        challenge_mission_handler.update_user_challenge_mission_data(
            challenge_key=challenge_key, mission_key=mission_key, mission_info=mission_info
        )

    return handler.response('', 201)

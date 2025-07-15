import asyncio
import boto3
import json
import os

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Union

from chalicelib.constants.common import DELETED, DEVICES, FREE, PAID, UPDATED_TIME_UTC
from chalicelib.slack_bot import post_slack_message


SERVER_ENV = os.getenv("SERVER_ENV")


def format_utc_timestamp(time_obj: datetime = None) -> str:
    if not time_obj:
        time_obj = datetime.now(timezone.utc)

    formatted_time = time_obj.strftime('%Y-%m-%d %I:%M:%S.') + time_obj.strftime('%f')[:3] + time_obj.strftime('%p')
    return formatted_time


def format_utc_timestamp_to_datetime(str_obj: str) -> datetime:
    try:
        formatted_time = datetime.strptime(str_obj, "%Y-%m-%d %I:%M:%S.%f%p")
    except ValueError:
        try:
            formatted_time = datetime.strptime(str_obj, "%Y-%m-%d %I:%M:%S%p")
        except ValueError:
            try:
                formatted_time = datetime.strptime(str_obj, '%Y-%m-%d %I.%M.%S.%f%p')
            except ValueError:
                try:
                    formatted_time = datetime.strptime(str_obj, "%Y-%m-%d %I.%M.%S%p")
                except ValueError:
                    formatted_time = datetime.strptime(str_obj, '%Y-%m-%d %I:%M%p')

    return formatted_time.replace(tzinfo=timezone.utc)


def format_utc_date_str() -> str:
    current_utc_time = datetime.now(timezone.utc)
    formatted_time = current_utc_time.strftime('%Y-%m-%d')
    return formatted_time


def format_kst_date_str() -> str:
    formatted_time = datetime.now().strftime('%Y-%m-%d')
    return formatted_time


def format_kst_timestamp_to_datetime(str_obj: str) -> datetime:
    try:
        formatted_time = datetime.strptime(str_obj, '%Y-%m-%d %I:%M%p') + timedelta(hours=9)
    except ValueError:
        formatted_time = datetime.strptime(str_obj, '%Y-%m-%d %I:%M:%S%p') + timedelta(hours=9)
    return formatted_time


def format_unix_timestamp(time_obj: datetime = None) -> float:
    if not time_obj:
        time_obj = datetime.now(timezone.utc)

    return time_obj.timestamp()


def create_change_log_data_set(
    ref_key: str, new_data: Union[str, int, bool, dict, list], old_data: Optional[Union[str, int, bool, dict]] = None
) -> dict:
    return {
        'EventName': f'{ref_key}Changed',
        'EventTimeUtc': format_utc_timestamp(),
        'OldData': old_data,
        'NewData': new_data,
    }


def manage_event_bridge_schedule(
    client: Any,
    operation: str,
    func_name: str,
    payload: dict,
    expression_time: datetime,
    group_name: str = 'default',
    name: str = '',
    timezone: str = 'Asia/Seoul',
) -> None:
    try:
        group_name = f'{group_name}-{SERVER_ENV}'
        response = client.list_schedule_groups(MaxResults=100)['ScheduleGroups']
        schedule_groups = [schedule_group.get('Name') for schedule_group in response]

        if group_name not in schedule_groups:
            client.create_schedule_group(Name=group_name)

        expression_time_str = f'at({expression_time.strftime("%Y-%m-%dT%H:%M:%S")})'
        schedule_info = {
            'FlexibleTimeWindow': {'Mode': 'OFF'},
            'ActionAfterCompletion': 'DELETE',
            'GroupName': group_name,
            'Name': name if name else datetime.now().strftime("%Y-%m-%dT%H-%M-%S.%f"),
            'ScheduleExpression': expression_time_str,
            'ScheduleExpressionTimezone': timezone,
            'Target': {
                'Arn': f'arn:aws:lambda:ap-northeast-2:\
{os.getenv("AWS_ACCOUNT_ID")}:function:fiva-api-server-{SERVER_ENV}-{func_name}',
                'Input': json.dumps(payload),
                'RoleArn': f'arn:aws:iam::{os.getenv("AWS_ACCOUNT_ID")}:role/EventBridgeScheduler-FCM',
                'RetryPolicy': {'MaximumEventAgeInSeconds': 60, 'MaximumRetryAttempts': 0},
            },
        }

        if operation == 'create':
            client.create_schedule(**schedule_info)
        elif operation == 'update':
            client.update_schedule(**schedule_info)

    except Exception as e:
        post_slack_message(
            channel_id=os.getenv('SLACK_DEV_CHANNEL_ID'),
            token=os.getenv('SLACK_TOKEN_FCM'),
            text=f'예약에 실패한 일정이 있습니다.\n\nExpression Time: {expression_time.strftime("%Y-%m-%dT%H:%M:%S")} \n\
Message Payload:\n```{payload}```',
        )
        print(e)


def get_active_user_profile(user_key: str, user_data: dict[str, Any]) -> Optional[dict]:
    user_profile = user_data.get(user_key)

    if user_profile is None or user_profile.get(DELETED):
        return None
    return user_profile


def check_subscribing_user(user_key: str, user_profile: dict[str, Any]) -> bool:
    now = datetime.now(timezone.utc)

    if not user_profile:
        return False

    subscription = user_profile.get('Subscription')
    free_pass = user_profile.get('FreePassEndTimeUtc')

    if subscription and free_pass:
        if subscription.get('ExpireDate'):
            subscription_expire_date = format_utc_timestamp_to_datetime(subscription['ExpireDate'])
            if now <= subscription_expire_date:
                return PAID

        free_pass_expire_date = format_utc_timestamp_to_datetime(free_pass)
        if now <= free_pass_expire_date:
            return FREE

    if subscription and subscription.get('ExpireDate'):
        subscription_expire_date = format_utc_timestamp_to_datetime(subscription['ExpireDate'])

    if free_pass:
        if now <= format_utc_timestamp_to_datetime(free_pass):
            return FREE

    return False


def create_activity_after_24_notification_schedule(user_id: str, payload=dict) -> None:
    expression_time = datetime.now() + timedelta(hours=23, minutes=30)
    group_name = 'ActivityAfter24NotificationGroup'
    client = boto3.client('scheduler')
    schedule_name = f'ActivityAfter24-{user_id}'

    try:
        client.get_schedule(GroupName=f'{group_name}-{SERVER_ENV}', Name=schedule_name)
    except client.exceptions.ResourceNotFoundException:
        return manage_event_bridge_schedule(
            client=client,
            operation='create',
            payload=payload,
            func_name='send_fcm_msg_func',
            expression_time=expression_time,
            group_name=group_name,
            name=schedule_name,
        )

    manage_event_bridge_schedule(
        client=client,
        operation='update',
        payload=payload,
        func_name='send_fcm_msg_func',
        expression_time=expression_time,
        group_name=group_name,
        name=schedule_name,
    )


def async_fetch_paths(root_ref, path_list):
    def fetch_data(path):
        return root_ref.child(path).get()

    async def fetch_all_data():
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            paths = path_list

            tasks = [loop.run_in_executor(executor, fetch_data, path) for path in paths]
            results = await asyncio.gather(*tasks)
            return dict(zip(paths, results))

    return asyncio.run(fetch_all_data())

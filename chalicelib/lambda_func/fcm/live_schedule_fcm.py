import json
import boto3

from datetime import datetime, timedelta
from typing import Any

from chalice import Blueprint

from chalicelib.core import format_unix_timestamp, manage_event_bridge_schedule
from chalicelib.constants.common import (
    ACTIVITY,
    BODY,
    CUSTOM_MESSAGE,
    DATA,
    FCM_METHOD,
    LIVE_TITLE,
    PUSH_KEY,
    PUSH_NOTIFICATION_OPENED,
    TITLE,
    TOPIC,
    VALUE,
)
from chalicelib.constants.db_ref_key import LIVE_SCHEDULE_INFO
from chalicelib.db.engine import root_ref
from chalicelib.firebase.core import create_fcm_datetime_topic, send_fcm_to_topic


live_schedule_fcm_module = Blueprint(__name__)

AM = 'AM'
PM = 'PM'

GROUP_NAME = 'LiveScheduleReserved'
INVOKE_FUNC_NAME = 'send_fcm_msg_func'


class LiveScheduleFCMHandler:
    def __init__(self, client, live_schedule: dict[str, Any]) -> None:
        self.today = datetime.now()
        self.reserving_target_date = self.today + timedelta(days=2)
        self.live_schedule = live_schedule
        self.client = client

        self.fcm_schedule = self._organize_tomorrow_fcm_schedule()

    def _organize_tomorrow_fcm_schedule(self) -> dict[str, dict[datetime, dict[str, Any]]]:
        schedule_month = self.reserving_target_date.strftime('%Y-%m')
        fcm_schedule = {AM: {}, PM: {}}

        for value in self.live_schedule.values():
            for key, value in value.get(schedule_month).items():
                datetime_key = datetime.strptime(key, '%Y-%m-%d %I:%M%p')

                if datetime_key.date() == self.reserving_target_date.date():
                    if datetime_key.hour < 12:
                        fcm_schedule[AM][datetime_key] = {
                            LIVE_TITLE: value.get(LIVE_TITLE),
                            CUSTOM_MESSAGE: value.get(CUSTOM_MESSAGE),
                        }
                    else:
                        fcm_schedule[PM][datetime_key] = {
                            LIVE_TITLE: value.get(LIVE_TITLE),
                            CUSTOM_MESSAGE: value.get(CUSTOM_MESSAGE),
                        }
        return fcm_schedule

    def _register_morning_schedule_fcm(self) -> None:
        morning_schedule = self.fcm_schedule[AM]

        remind_datetime = (self.reserving_target_date - timedelta(days=1)).replace(
            hour=21, minute=40, second=0, microsecond=0
        )
        for datetime_key, value in morning_schedule.items():
            topic = create_fcm_datetime_topic(datetime_key)
            name_previous_day = f'{datetime.strftime(datetime_key, "%Y%m%d-%H")}-PreviousDay'

            fcm_data = {
                PUSH_NOTIFICATION_OPENED: json.dumps(
                    {
                        PUSH_KEY: GROUP_NAME,
                        ACTIVITY: 'Morning',
                        VALUE: {'AlertTime': 'PreviousDay', 'SendTimeUtc': format_unix_timestamp()},
                    }
                )
            }
            remind_payload = {
                FCM_METHOD: TOPIC,
                TITLE: '📣내일 운동 예약 알림',
                BODY: f'내일 아침 {datetime_key.hour}시 클래스에서 만나요! 🥳',
                TOPIC: topic,
                DATA: fcm_data,
            }

            manage_event_bridge_schedule(
                client=self.client,
                operation='create',
                func_name=INVOKE_FUNC_NAME,
                payload=remind_payload,
                expression_time=remind_datetime,
                group_name=GROUP_NAME,
                name=name_previous_day,
            )

            name_10_minutes_ago = f'{datetime.strftime(datetime_key, "%Y%m%d-%H")}-10MinutesAgo'
            fcm_data = {
                PUSH_NOTIFICATION_OPENED: json.dumps(
                    {
                        PUSH_KEY: GROUP_NAME,
                        ACTIVITY: 'Morning',
                        VALUE: {'AlertTime': '10MinutesAgo', 'SendTimeUtc': format_unix_timestamp()},
                    }
                )
            }
            recent_remind_payload = {
                FCM_METHOD: TOPIC,
                TITLE: '10분 뒤에 FIVA 에서 만나요!🐥',
                BODY: (
                    value[CUSTOM_MESSAGE].replace('\\n', '\n')
                    if value[CUSTOM_MESSAGE] is not None
                    else value[LIVE_TITLE]
                ),
                TOPIC: topic,
                DATA: fcm_data,
            }

            manage_event_bridge_schedule(
                client=self.client,
                operation='create',
                func_name=INVOKE_FUNC_NAME,
                payload=recent_remind_payload,
                expression_time=datetime_key - timedelta(minutes=10),
                group_name=GROUP_NAME,
                name=name_10_minutes_ago,
            )

    def _register_afternoon_schedule_fcm(self) -> None:
        afternoon_schedule = self.fcm_schedule[PM]

        remind_datetime = self.reserving_target_date.replace(hour=9, minute=0, second=0, microsecond=0)
        for datetime_key, value in afternoon_schedule.items():
            topic = create_fcm_datetime_topic(datetime_key)
            name_day_morning = f'{datetime.strftime(datetime_key, "%Y%m%d-%H")}-DayMorning'

            fcm_data = {
                PUSH_NOTIFICATION_OPENED: json.dumps(
                    {
                        PUSH_KEY: GROUP_NAME,
                        ACTIVITY: 'Afternoon',
                        VALUE: {'AlertTime': 'DayMorning', 'SendTimeUtc': format_unix_timestamp()},
                    }
                )
            }
            remind_payload = {
                FCM_METHOD: TOPIC,
                TITLE: '📣오늘 운동 예약 알림',
                BODY: f'오늘 저녁 {datetime_key.hour - 12}시 클래스에서 만나요! 🥳',
                TOPIC: topic,
                DATA: fcm_data,
            }

            manage_event_bridge_schedule(
                client=self.client,
                operation='create',
                func_name=INVOKE_FUNC_NAME,
                payload=remind_payload,
                expression_time=remind_datetime,
                group_name=GROUP_NAME,
                name=name_day_morning,
            )

            name_30_minutes_ago = f'{datetime.strftime(datetime_key, "%Y%m%d-%H")}-30MinutesAgo'
            fcm_data = {
                PUSH_NOTIFICATION_OPENED: json.dumps(
                    {
                        PUSH_KEY: GROUP_NAME,
                        ACTIVITY: 'Afternoon',
                        VALUE: {'AlertTime': '30MinutesAgo', 'SendTimeUtc': format_unix_timestamp()},
                    }
                )
            }
            recent_remind_payload = {
                FCM_METHOD: TOPIC,
                TITLE: '30분 뒤에 FIVA 에서 만나요!🐥',
                BODY: (
                    value[CUSTOM_MESSAGE].replace('\\n', '\n')
                    if value[CUSTOM_MESSAGE] is not None
                    else value[LIVE_TITLE]
                ),
                TOPIC: topic,
                DATA: fcm_data,
            }

            manage_event_bridge_schedule(
                client=self.client,
                operation='create',
                func_name=INVOKE_FUNC_NAME,
                payload=recent_remind_payload,
                expression_time=datetime_key - timedelta(minutes=30),
                group_name=GROUP_NAME,
                name=name_30_minutes_ago,
            )

    def _register_common_schedule_fcm(self) -> None:
        fcm_data = {
            PUSH_NOTIFICATION_OPENED: json.dumps(
                {
                    PUSH_KEY: GROUP_NAME,
                    ACTIVITY: 'Common',
                    VALUE: {'AlertTime': '1MinutesAgo', 'SendTimeUtc': format_unix_timestamp()},
                }
            )
        }
        for value in self.fcm_schedule.values():
            for datetime_key, value in value.items():
                topic = create_fcm_datetime_topic(datetime_key)
                name_1_minutes_ago = f'{datetime.strftime(datetime_key, "%Y%m%d-%H")}-1MinutesAgo'

                remind_payload = {
                    FCM_METHOD: TOPIC,
                    TITLE: '오늘 운동 시작!!',
                    BODY: '얼른 와서 함께 운동해요🙆‍♀️',
                    TOPIC: topic,
                    DATA: fcm_data,
                }

                manage_event_bridge_schedule(
                    client=self.client,
                    operation='create',
                    func_name=INVOKE_FUNC_NAME,
                    payload=remind_payload,
                    expression_time=datetime_key - timedelta(minutes=1),
                    group_name=GROUP_NAME,
                    name=name_1_minutes_ago,
                )

    def register_live_schedule_fcm(self) -> None:
        self._register_morning_schedule_fcm()
        self._register_afternoon_schedule_fcm()
        self._register_common_schedule_fcm()


@live_schedule_fcm_module.schedule('cron(0 15 * * ? *)')
def schedule_fcm_msg(event) -> None:
    client = None

    try:
        live_schedule_ref = root_ref.child(LIVE_SCHEDULE_INFO)

        live_schedule = live_schedule_ref.get()
        if not live_schedule:
            return None

        client = boto3.client('scheduler')
        LiveScheduleFCMHandler(client=client, live_schedule=live_schedule).register_live_schedule_fcm()

    except Exception as e:
        print(e)

    finally:
        if client is not None:
            client.close()


@live_schedule_fcm_module.lambda_function()
def send_fcm_msg(event, context) -> None:
    try:
        send_fcm_to_topic(topic=event['topic'], title=event['title'], body=event['body'])

    except Exception as e:
        print(e)

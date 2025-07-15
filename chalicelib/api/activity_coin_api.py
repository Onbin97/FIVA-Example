import json
import os
import requests
import time

from typing import Optional

from chalice import Blueprint, CORSConfig
from chalice import BadRequestError
from chalice.app import Request, Response
from firebase_admin.db import Reference

from chalicelib.api_setup import APIHandler, common_set_up
from chalicelib.core import check_subscribing_user, format_utc_timestamp
from chalicelib.constants.common import (
    ACTIVITY,
    ACTIVITY_COIN,
    COINS,
    COLLECTED_CURRENCY,
    DATE_KEY,
    EVENT_TIME_UTC,
    FREE,
    PAID,
    PHONE_NUMBER,
)
from chalicelib.constants.db_ref_key import (
    DB_ACTIVITY_COIN_LOGS_DATE_GROUPED,
    DB_BETA_USER_ACTIVITY_COIN_LOGS,
    DB_BETA_USER_DATA,
    DB_METHODS_OF_ACTIVITY_COIN_ACQUISITION,
)
from chalicelib.validation import is_valid_phone_number


activity_coin_api_module = Blueprint(__name__)

ACQUISITION_FINISHED = 'AcquisitionFinished'

cors_config = CORSConfig(allow_origin='*')


class ActivityCoinAPIHandler:
    def __init__(self, root_ref: Reference, user_id: str, activity: str) -> None:
        self.acquisition_status = {ACQUISITION_FINISHED: False}
        self.root_ref = root_ref
        self.user_id = user_id
        self.activity = activity

        self.user_data_ref = root_ref.child(DB_BETA_USER_DATA)
        self.user_activity_coin_logs_ref = root_ref.child(DB_BETA_USER_ACTIVITY_COIN_LOGS).child(user_id)

    def update_user_activity_coins(self, coins: int) -> Optional[dict]:
        activity_coin_data = {}

        def transaction_update(current_data):
            if current_data is None:
                current_data = 0

            if current_data + coins < 0:
                raise BadRequestError('Not enough coins')

            activity_coin_data.update(
                {
                    COINS: coins,
                    'BeforeCoins': current_data,
                    'AfterCoins': (current_data + coins),
                    ACTIVITY: self.activity,
                    **self.acquisition_status,
                    EVENT_TIME_UTC: format_utc_timestamp(),
                }
            )
            return current_data + coins

        result = (
            self.user_data_ref.child(self.user_id)
            .child(COLLECTED_CURRENCY)
            .child(ACTIVITY_COIN)
            .transaction(transaction_update)
        )

        if result is not None:
            self.logging_activity_coin_data(activity_coin_data=activity_coin_data)
            return activity_coin_data

    def logging_activity_coin_data(self, activity_coin_data: dict) -> None:
        self.user_activity_coin_logs_ref.push().set(activity_coin_data)


class ActivityCoinAcquisitionHandler(ActivityCoinAPIHandler):
    def __init__(self, root_ref: Reference, user_id: str, date_key: str, activity: str, count: int = 0) -> None:
        super().__init__(root_ref, user_id, activity)

        self.count = count
        self.date_key = date_key
        self.user_profile = self.root_ref.child(DB_BETA_USER_DATA).child(self.user_id).get()
        self.sub = FREE if not check_subscribing_user(user_key=user_id, user_profile=self.user_profile) else PAID
        self.methods_of_coin_acquisition = (
            self.root_ref.child(DB_METHODS_OF_ACTIVITY_COIN_ACQUISITION).child(self.sub).get()
        )

        if self.activity not in self.methods_of_coin_acquisition:
            raise BadRequestError('Invalid activity')

        self.value_per = self.methods_of_coin_acquisition[self.activity]['ValuePer']
        self.activity_coin_logs_date_grouped_ref = root_ref.child(DB_ACTIVITY_COIN_LOGS_DATE_GROUPED).child(
            self.date_key
        )

    def get_remaining_coins(self) -> bool:
        activity_coin_logs_date_grouped = self.activity_coin_logs_date_grouped_ref.get()
        max_coins = self.methods_of_coin_acquisition[self.activity]['DailyMaxValue']

        if not activity_coin_logs_date_grouped or self.user_id not in activity_coin_logs_date_grouped:
            return max_coins

        user_daily_logs = activity_coin_logs_date_grouped[self.user_id]
        user_daily_activity_coins = sum(
            [
                daily_log[COINS]
                for daily_log in user_daily_logs.values()
                if isinstance(daily_log, dict) and daily_log[ACTIVITY] == self.activity
            ]
        )

        if user_daily_activity_coins >= max_coins:
            return 0
        return max_coins - user_daily_activity_coins

    def calculate_coins(self, remaining_coins: int) -> int:
        coins = self.value_per * self.count

        if remaining_coins <= coins:
            self.acquisition_status[ACQUISITION_FINISHED] = True
            return remaining_coins
        return coins

    def logging_activity_coin_acquisition_date_grouped_data(self, activity_coin_data: dict) -> None:
        self.activity_coin_logs_date_grouped_ref.child(self.user_id).push().set(activity_coin_data)


class ActivityCoinConsumptionHandler(ActivityCoinAPIHandler):
    def __init__(self, root_ref: Reference, user_id: str, activity: str, coins: int) -> None:
        super().__init__(root_ref, user_id, activity)

        self.user_profile = self.root_ref.child(DB_BETA_USER_DATA).child(self.user_id).get()
        self.coins = -coins

    def has_enough_coins(self):
        user_coins = self.user_profile.get(COLLECTED_CURRENCY, {}).get(ACTIVITY_COIN, 0)
        if user_coins + self.coins < 0:
            return False
        return True

    def send_kakao_gift(self, gift_id: str, phone_number: str) -> dict:
        KAKAO_GIFT_URL = "https://gateway-giftbiz.kakao.com/openapi/giftbiz/v1/template/order"

        kakao_gift_api_key = os.getenv('KAKAO_GIFT_API_KEY')

        gift_data = self.root_ref.child('exchangeable_gift_catalog').child(gift_id).get()
        if not gift_data:
            raise BadRequestError('The gift information does not exist.')

        temp_token = gift_data['TempToken']
        gift_coins = gift_data['Prices'][ACTIVITY_COIN]

        if self.coins + gift_coins != 0:

            raise BadRequestError('The coin amount does not match')

        if not gift_id or not phone_number:
            raise BadRequestError('Invalid gift information')

        external_order_id = f'{self.user_id}_{str(time.time()).replace(".", "_")}'

        payload = {
            'receiver_type': 'PHONE',
            'receivers': [{'external_key': self.user_id, 'name': self.user_id, 'receiver_id': phone_number}],
            'template_token': temp_token,
            'external_order_id': external_order_id,
        }
        headers = {
            'accept': 'application/json',
            'Authorization': f'KakaoAK {kakao_gift_api_key}',
            'content-type': 'application/json',
        }

        response = requests.post(url=KAKAO_GIFT_URL, json=payload, headers=headers)
        parsed_res = json.loads(response.text)

        if response.status_code == 200:
            result = self.update_user_activity_coins(self.coins)
            reserve_trace_id = parsed_res['reserve_trace_id']

            gift_info = {
                'ReserveTraceId': reserve_trace_id,
                'GiftData': gift_data,
                'ExternalOrderId': external_order_id,
                PHONE_NUMBER: parsed_res['template_receivers'][0]['receiver_id'],
                COINS: self.coins,
                EVENT_TIME_UTC: format_utc_timestamp(),
            }
            self.root_ref.child('beta_user_kakao_gift_logs').child(self.user_id).child(external_order_id).set(gift_info)
            result['GiftInfo'] = {
                'ReserveTraceId': reserve_trace_id,
                'ExternalOrderId': external_order_id,
                'GiftId': gift_id,
                **gift_data,
            }
            return result
        else:
            raise BadRequestError(response.text)


@activity_coin_api_module.route('/activity-coin/acquisition', methods=['POST', 'GET'], cors=cors_config)
@common_set_up(module=activity_coin_api_module)
def activity_coin_acquisition_api(request: Request, root_ref: Reference, handler: APIHandler) -> Response:
    user_id = request.query_params.get('UserId')
    if not user_id:
        raise BadRequestError('Missing user ID in the request')

    if request.method == 'POST':
        date_key = request.json_body.get(DATE_KEY)
        if not date_key:
            raise BadRequestError('Missing date key in the request')

        activity = request.json_body.get(ACTIVITY)
        if not activity:
            raise BadRequestError('Missing category in the request')

        count = request.json_body.get('Count') or 0

        activity_coin_api_handler = ActivityCoinAcquisitionHandler(
            root_ref=root_ref, user_id=user_id, activity=activity, count=count, date_key=date_key
        )
        remaining_coins = activity_coin_api_handler.get_remaining_coins()

        if not remaining_coins:
            return handler.response({COINS: 0, ACQUISITION_FINISHED: True, EVENT_TIME_UTC: handler.timestamp}, 201)

        result = activity_coin_api_handler.update_user_activity_coins(
            coins=activity_coin_api_handler.calculate_coins(remaining_coins=remaining_coins)
        )
        if result:
            activity_coin_api_handler.logging_activity_coin_acquisition_date_grouped_data(result)

        return handler.response(result, 201)

    if request.method == 'GET':
        date_key = request.query_params.get(DATE_KEY)
        if not date_key:
            raise BadRequestError('Missing date key in the request')

        activity = request.query_params.get(ACTIVITY)
        if not activity:
            raise BadRequestError('Missing category in the request')

        activity_coin_api_handler = ActivityCoinAcquisitionHandler(
            root_ref=root_ref, user_id=user_id, activity=activity, date_key=date_key
        )
        remaining_coins = activity_coin_api_handler.get_remaining_coins()

        result = {
            ACTIVITY: activity,
            'RemainingCoins': remaining_coins,
            'ValuePer': activity_coin_api_handler.value_per,
        }

        return handler.response(result, 200)


@activity_coin_api_module.route('/activity-coin/consumption', methods=['POST'])
@common_set_up(module=activity_coin_api_module)
def activity_coin_consumption_api(request: Request, root_ref: Reference, handler: APIHandler) -> Response:
    user_id = request.query_params.get('UserId')
    if not user_id:
        raise BadRequestError('Missing user ID in the request')

    body = request.json_body
    if not body:
        raise BadRequestError('Missing body in the request')

    activity = body.get(ACTIVITY)
    if not activity:
        raise BadRequestError('Missing category in the request')

    coins = body.get(COINS)
    if not coins:
        raise BadRequestError('Missing coins in the request')

    activity_coin_api_handler = ActivityCoinConsumptionHandler(
        root_ref=root_ref, user_id=user_id, activity=activity, coins=coins
    )
    if not activity_coin_api_handler.has_enough_coins():
        raise BadRequestError('Not enough coins')

    if activity == 'KakaoGift':
        phone_number = body.get(PHONE_NUMBER, '')
        if not is_valid_phone_number(phone_number):
            raise BadRequestError('Invalid phone number')

        gift_id = body.get('GiftId')
        if not gift_id:
            raise BadRequestError('Invalid gift id')

        return activity_coin_api_handler.send_kakao_gift(gift_id, phone_number)

    result = activity_coin_api_handler.update_user_activity_coins(coins=activity_coin_api_handler.coins)
    return handler.response(result, 201)

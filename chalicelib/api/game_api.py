import os

from datetime import datetime, time, timedelta, timezone
from firebase_admin.db import Reference

from chalice import Blueprint, Rate
from chalice import BadRequestError
from chalice.app import Request, Response

from chalicelib.api_setup import APIHandler, common_set_up
from chalicelib.core import (
    async_fetch_paths,
    get_active_user_profile,
    format_utc_timestamp,
    format_utc_timestamp_to_datetime,
)
from chalicelib.constants.common import (
    COSTUME_LIST,
    FULL,
    GAME_OVER_TIME_UTC,
    HALF,
    NICKNAME,
    RANK,
    SKI_GAME,
    UPDATED_TIME_UTC,
    USER_KEY,
    USER_LIST,
    WEEK_START_TIME_UTC,
)
from chalicelib.constants.db_ref_key import (
    DB_BETA_USER_DATA,
    DB_BETA_USER_GAME_LOGS,
    DB_GAME_RANkING_CURRENT_WEEK,
    DB_GAME_RANkING_LAST_WEEK,
)
from chalicelib.db.engine import root_ref
from chalicelib.slack_bot import post_slack_message


game_api_module = Blueprint(__name__)


GAME_MAP = {'ski': SKI_GAME, 'arm-flight': 'ArmFlightGame'}


class FivaGameHandler:
    def __init__(self, game_name) -> None:
        self.today = datetime.now()
        self.today_utc = datetime.now(tz=timezone.utc)
        self.weekday = datetime.combine(self.today.date(), time.min, tzinfo=timezone.utc) - timedelta(
            days=((self.today.weekday()) % 7), hours=9
        )
        self.target_game_name = game_name

        data = async_fetch_paths(
            root_ref,
            [DB_BETA_USER_DATA, DB_BETA_USER_GAME_LOGS, f'{DB_GAME_RANkING_CURRENT_WEEK}/{self.target_game_name}'],
        )
        self.user_data = data[DB_BETA_USER_DATA]
        self.game_logs = data[DB_BETA_USER_GAME_LOGS]
        self.current_week_ranking_data = data[f'{DB_GAME_RANkING_CURRENT_WEEK}/{self.target_game_name}']

    def calculate_current_week_rank(self):
        if self.current_week_ranking_data and self.weekday - format_utc_timestamp_to_datetime(
            self.current_week_ranking_data[WEEK_START_TIME_UTC]
        ) >= timedelta(days=7):
            self._update_last_week_ranking_data(current_week_ranking_data=self.current_week_ranking_data)

        ranking_data = {
            WEEK_START_TIME_UTC: format_utc_timestamp(self.weekday),
            UPDATED_TIME_UTC: format_utc_timestamp(self.today_utc),
        }

        if not self.game_logs:
            return ranking_data

        target_game_logs = {
            user_key: game_log[self.target_game_name]
            for user_key, game_log in self.game_logs.items()
            if game_log.get(self.target_game_name)
        }

        high_score_data = []
        for user_key, target_game_log in target_game_logs.items():
            if not get_active_user_profile(user_key, self.user_data):
                continue

            weekly_logs = [
                log
                for log in target_game_log.values()
                if isinstance(log, dict) and format_utc_timestamp_to_datetime(log[GAME_OVER_TIME_UTC]) >= self.weekday
            ]

            if not weekly_logs:
                continue

            high_score_log = max(
                weekly_logs,
                key=lambda log: (
                    log[FULL] + (log[HALF] * 0.5),
                    format_utc_timestamp_to_datetime(log[GAME_OVER_TIME_UTC]),
                ),
            )

            high_score_log[USER_KEY] = user_key
            high_score_log[NICKNAME] = self.user_data[user_key].get(NICKNAME)
            high_score_log[COSTUME_LIST] = self.user_data[user_key].get(COSTUME_LIST)
            high_score_data.append(high_score_log)

        high_score_data.sort(
            key=lambda x: (-(x[FULL] + (x[HALF] * 0.5)), format_utc_timestamp_to_datetime(x[GAME_OVER_TIME_UTC]))
        )
        for index, log in enumerate(high_score_data):
            log[RANK] = index + 1

        ranking_data[USER_LIST] = high_score_data
        self._update_current_week_ranking_data(ranking_data)

    def _update_current_week_ranking_data(self, ranking_data: dict) -> None:
        root_ref.child(DB_GAME_RANkING_CURRENT_WEEK).child(self.target_game_name).update(ranking_data)

    def _update_last_week_ranking_data(self, current_week_ranking_data: dict) -> None:
        current_week_ranking_data[UPDATED_TIME_UTC] = format_utc_timestamp(self.today_utc)

        root_ref.child(DB_GAME_RANkING_LAST_WEEK).child(self.target_game_name).update(
            {current_week_ranking_data[WEEK_START_TIME_UTC].split(' ')[0]: current_week_ranking_data}
        )


@game_api_module.route('/games/{game_name}', methods=['PUT'])
@common_set_up(module=game_api_module)
def game_rank_api(request: Request, root_ref: Reference, handler: APIHandler, game_name: str) -> Response:
    if game_name not in GAME_MAP:
        raise BadRequestError(f'Invalid game name: {game_name}')

    FivaGameHandler(GAME_MAP[game_name]).calculate_current_week_rank()

    return handler.response('', 200)


if os.getenv('SERVER_ENV') == 'prod':

    @game_api_module.schedule(Rate(6, Rate.HOURS))
    def schedule_game_rank(event) -> None:
        try:
            for game_name in GAME_MAP.values():
                FivaGameHandler(game_name).calculate_current_week_rank()

        except Exception as e:
            post_slack_message(
                channel_id=os.getenv('SLACK_DEV_CHANNEL_ID'),
                token=os.getenv('SLACK_TOKEN_SERVER'),
                text=f'{game_name} Ranking Data Update Failed 🚨\n\nError Message:\n```ERROR: {e}```',
            )
            print(e)

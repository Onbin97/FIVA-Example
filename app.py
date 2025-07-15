import os
from chalice import Chalice

from chalicelib.api import (
    groups_api,
    activity_coin_api,
    stair_climbing_api,
    test_api,
    user_api,
    rewards_api,
    workout_logs_api,
    fcm_api,
    challenge_api,
    game_api,
)
from chalicelib.lambda_func import mixpanel_migration, slack
from chalicelib.lambda_func.fcm import default_fcm, live_schedule_fcm

server_env = os.getenv('SERVER_ENV')
app = Chalice(app_name=f'fiva-{server_env}')


# FIVA API
app.register_blueprint(workout_logs_api.workout_logs_api_module)
app.register_blueprint(stair_climbing_api.stair_climbing_api_module)
app.register_blueprint(activity_coin_api.activity_coin_api_module)
app.register_blueprint(fcm_api.fcm_api_module)
app.register_blueprint(challenge_api.challenge_api_module)
app.register_blueprint(game_api.game_api_module)

# Lambda Func
app.register_blueprint(live_schedule_fcm.live_schedule_fcm_module)
app.register_blueprint(default_fcm.default_fcm_module)

if server_env == 'prod':
    app.register_blueprint(mixpanel_migration.mixpanel_migration_module)
    app.register_blueprint(slack.fiva_slack_module)

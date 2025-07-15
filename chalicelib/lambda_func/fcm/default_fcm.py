from chalice import Blueprint

from chalicelib.constants.common import (
    BODY,
    DATA,
    DELETED,
    DEVICES,
    FCM_METHOD,
    MULTICAST,
    NICKNAME,
    TITLE,
    TOKEN,
    TOPIC,
    USER_DEVICES,
    USER_KEY,
)
from chalicelib.constants.db_ref_key import DB_BETA_USER_DATA
from chalicelib.db.engine import root_ref
from chalicelib.firebase.core import send_fcm, send_fcm_multicast, send_fcm_to_topic


default_fcm_module = Blueprint(__name__)


@default_fcm_module.lambda_function()
def send_fcm_msg_func(event, context) -> None:
    if FCM_METHOD not in event:
        return 'Invalid fcm method'

    title = event[TITLE]
    body = event[BODY]
    data = event.get(DATA)

    if event[FCM_METHOD] == TOPIC:
        topic = event.get(TOPIC)
        if not topic:
            return 'Invalid topic'
        return send_fcm_to_topic(topic=topic, title=title, body=body, data=data)

    if event[FCM_METHOD] == TOKEN:
        token = event.get(TOKEN)
        if not token:
            return 'Invalid token'
        return send_fcm(token=token, title=title, body=body, data=data)

    if event[FCM_METHOD] == MULTICAST:
        token_list = event.get('token_list')
        if not token:
            return 'Invalid token list'
        return send_fcm_multicast(tokens=token_list, title=title, body=body, data=data)

    if event[FCM_METHOD] == USER_DEVICES:
        user_id = event.get(USER_KEY)
        if not user_id:
            return 'Invalid user id'
        user_profile = root_ref.child(DB_BETA_USER_DATA).child(user_id).get()
        if user_profile is None or user_profile.get(DELETED) or not isinstance(user_profile.get(DEVICES), dict):
            return 'Invalid user'

        nickname = user_profile.get(NICKNAME, '')
        devices = user_profile.get(DEVICES)

        token_list = [device[TOKEN] for device in devices.values() if device.get(TOKEN)]

        title = title.replace('{Nickname}', nickname)
        body = body.replace('{Nickname}', nickname)

        return send_fcm_multicast(tokens=token_list, title=title, body=body, data=data)

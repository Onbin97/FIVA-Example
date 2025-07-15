import os

from datetime import datetime

from firebase_admin import messaging
from firebase_admin._messaging_utils import UnregisteredError

from chalicelib.slack_bot import post_slack_message


def create_fcm_datetime_topic(fcm_datetime: datetime) -> str:
    date_str = fcm_datetime.strftime('%y%m%d')
    time_str = fcm_datetime.strftime('%H')

    topic = f'schedule_{date_str}_{time_str}'
    if not os.getenv('SERVER_ENV') == 'prod':
        topic = 'test_' + topic

    return topic


def send_fcm(token: str, title: str, body: str, user_id: str, category: str = None, subtitle: str = None) -> None:
    try:
        message = messaging.Message(
            notification=messaging.Notification(title=title, body=body),
            apns=messaging.APNSConfig(
                payload=messaging.APNSPayload(
                    aps=messaging.Aps(
                        alert=messaging.ApsAlert(title=title, subtitle=subtitle, body=body),
                        sound='default',
                        content_available=True,
                    )
                )
            ),
            token=token,
        )

        messaging.send(message)

    except UnregisteredError:
        return
    except Exception as e:
        post_slack_message(
            channel_id=os.getenv('SLACK_DEV_CHANNEL_ID'),
            token=os.getenv('SLACK_TOKEN_FCM'),
            text=f'[{category}] 메시지 전송에 실패하였습니다.\n\nToken: {token}\nUID: {user_id}',
        )
        print(e)


def send_fcm_to_topic(topic: str, title: str, body: str) -> None:
    try:
        message = messaging.Message(
            notification=messaging.Notification(title=title, body=body),
            apns=messaging.APNSConfig(
                payload=messaging.APNSPayload(aps=messaging.Aps(sound='default', content_available=True))
            ),
            topic=topic,
        )
        messaging.send(message)

    except Exception as e:
        post_slack_message(
            channel_id=os.getenv('SLACK_DEV_CHANNEL_ID'),
            token=os.getenv('SLACK_TOKEN_FCM'),
            text=f'전송에 실패한 구독 메시지가 있습니다. \n\nTopic: {topic}',
        )
        print(e)


def send_fcm_multicast(
    tokens: list[str], title: str, body: str, data: dict = None, subtitle: str = None, category: str = ''
) -> None:
    CHUNK_SIZE = 500
    try:
        failed_tokens = []
        for i in range(0, len(tokens), CHUNK_SIZE):
            token_chunk = tokens[i : i + CHUNK_SIZE]

            message = messaging.MulticastMessage(
                notification=messaging.Notification(title=title, body=body),
                apns=messaging.APNSConfig(
                    payload=messaging.APNSPayload(
                        aps=messaging.Aps(
                            alert=messaging.ApsAlert(title=title, subtitle=subtitle, body=body),
                            sound='default',
                            content_available=True,
                        )
                    )
                ),
                data=data,
                tokens=token_chunk,
            )

            batch_response = messaging.send_each_for_multicast(message)

            if batch_response.failure_count > 0:
                responses = batch_response.responses
                for idx, resp in enumerate(responses):
                    if not resp.success:
                        failed_tokens.append({'token': token_chunk[idx], 'error': str(resp.exception)})

    except UnregisteredError:
        return
    except Exception as e:
        if failed_tokens:
            print("Failed tokens and errors:")
            for failure in failed_tokens:
                print(f"[{category}] Token: {failure['token']}, Error: {failure['error']}")

        print(e)
        post_slack_message(
            channel_id=os.getenv('SLACK_DEV_CHANNEL_ID'),
            token=os.getenv('SLACK_TOKEN_FCM'),
            text=f'[{category}] 메시지 전송에 실패하였습니다.',
        )

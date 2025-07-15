import certifi
import os
import ssl

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

ssl._create_default_https_context = ssl._create_unverified_context
ssl_context = ssl.create_default_context(cafile=certifi.where())


def post_slack_message(channel_id: str, token: str, text: str, blocks: list = None) -> None:
    server_env = os.getenv('SERVER_ENV')
    if server_env == 'dev':
        return
    msg_text = f'({server_env}) ' + text
    client = WebClient(token=token, ssl=ssl_context)

    try:
        client.chat_postMessage(channel=channel_id, text=msg_text, blocks=blocks)

    except SlackApiError as e:
        print(e)
        assert e.response["error"]

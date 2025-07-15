import os

from chalice import Blueprint

from chalicelib.db.engine import root_ref
from chalicelib.constants.common import CONTENT_KEY, NICKNAME
from chalicelib.constants.db_ref_key import DB_CONTENT_FEEDBACK, DB_CONTENT_INFO
from chalicelib.slack_bot import post_slack_message


fiva_slack_module = Blueprint(__name__)

SENT = 'Sent'


@fiva_slack_module.schedule('cron(40 * * * ? *)')
def schedule_content_feedback_alert(event) -> None:
    content_feedback_ref = root_ref.child(DB_CONTENT_FEEDBACK)
    content_info = root_ref.child(DB_CONTENT_INFO).get()
    teacher_info = root_ref.child('teacher_info').get()
    content_feedback = content_feedback_ref.get()

    for feedback_info in content_feedback.values():
        nickname = feedback_info.get(NICKNAME)
        feedback_list = feedback_info.get('ResultTypesKor')

        if not feedback_info.get(SENT) and nickname and feedback_list:
            content_key = feedback_info.get(CONTENT_KEY, '')
            feedback_str = ', '.join(feedback_list)

            if content_key not in content_info['LiveList']:
                blocks = [
                    {"type": "section", "text": {"type": "mrkdwn", "text": f"*컨텐츠:* `{content_key}`"}},
                    {
                        "type": "section",
                        "text": {"type": "mrkdwn", "text": f"*피드백:* {feedback_str} / *닉네임:* `{nickname}`\n"},
                    },
                    {"type": "divider"},
                ]

            else:
                content = content_info['LiveList'][content_key]
                teacher_key = content.get('TeacherKey', '')
                teacher = teacher_info.get(teacher_key, {}).get('Name')
                live_type = 'Streaming'

                if not content.get('IsStreaming'):
                    live_type = 'Vod'
                    content = content_info['VodList'][content['VodContentJoinKey']]

                blocks = [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"*제목:* `{content.get('TitleText', '')}`, *강사명:* `{teacher}`, \
*라이브 타입:* `{live_type}`",
                        },
                    },
                    {
                        "type": "section",
                        "text": {"type": "mrkdwn", "text": f"*피드백:* {feedback_str} / *닉네임:* `{nickname}`\n"},
                    },
                    {"type": "divider"},
                ]

            post_slack_message(
                channel_id=os.getenv('SLACK_CONTENT_REVIEW_BOT_CHANNEL_ID'),
                token=os.getenv('SLACK_TOKEN_SERVER'),
                text='컨텐츠 리뷰 알림',
                blocks=blocks,
            )

            feedback_info[SENT] = True

    content_feedback_ref.update(content_feedback)

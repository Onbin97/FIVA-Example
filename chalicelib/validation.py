import re


def is_valid_phone_number(phone_number):
    # 정규 표현식 패턴: 010-0000-0000 또는 01000000000
    pattern = r'^010-?\d{4}-?\d{4}$'
    return bool(re.match(pattern, phone_number))

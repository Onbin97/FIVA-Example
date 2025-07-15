import functools

import watchtower

from logging import Formatter, getLogger, INFO, Logger

from chalice import BadRequestError, NotFoundError
from chalice import Blueprint
from chalice.app import Request, Response

from chalicelib.core import format_utc_timestamp
from chalicelib.db.engine import root_ref


class APIHandler:
    def __init__(self, request: Request):
        self.logger = self._create_logger()
        self.request = request
        self.timestamp = format_utc_timestamp()

    def _create_logger(self) -> Logger:
        logger = getLogger()
        logger.setLevel(INFO)

        handler = watchtower.CloudWatchLogHandler()
        formatter = Formatter('[%(levelname)s] %(message)s')

        handler.setFormatter(formatter)
        logger.addHandler(handler)

        return logger

    def logging_request_info(self) -> None:
        context = self.request.context

        if self.request._body:
            context['requestBody'] = self.request.json_body
        if self.request.query_params:
            context['queryParams'] = dict(self.request.query_params)

        self.logger.info(context)

    def response(self, body: dict, status_code: int) -> Response:
        context = self.request.context
        response_log_info = {
            'body': body,
            'statusCode': status_code,
            'requestBody': context.get('requestBody', ''),
            'queryParams': context.get('queryParams', ''),
            'requestId': context.get('requestId', ''),
            'authorizer': context.get('authorizer', {}),
        }

        self.logger.info(response_log_info)

        return Response(
            body=body,
            status_code=status_code,
            headers={
                'Content-Type': 'application/json',
                'Request-Id': context.get('requestId', ''),
                'Resource-Path': context.get('resourcePath', ''),
            },
        )

    def error(self, error: str, status_code: int) -> Response:
        context = self.request.context
        error_log_info = {
            'message': str(error),
            'statusCode': status_code,
            'requestBody': context.get('requestBody', ''),
            'queryParams': context.get('queryParams', ''),
            'requestId': context.get('requestId', ''),
            'authorizer': context.get('authorizer', {}),
        }

        self.logger.error(error_log_info)

        return Response(
            body={'Error': str(error)},
            status_code=status_code,
            headers={
                'Content-Type': 'application/json',
                'Request-Id': context.get('requestId', ''),
                'Resource-Path': context.get('resourcePath', ''),
            },
        )


def common_set_up(module: Blueprint):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(**kwargs):
            try:
                request = module.current_request

                handler = APIHandler(request)
                handler.logging_request_info()

                uri_params = request.uri_params or {}
                kwargs.update(uri_params)

                return func(request=request, root_ref=root_ref, handler=handler, **kwargs)

            except BadRequestError as e:
                return handler.error(e, 400)
            except NotFoundError as e:
                return handler.error(e, 404)
            except Exception as e:
                return handler.error(e, 500)

        return wrapper

    return decorator

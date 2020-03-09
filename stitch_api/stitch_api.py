from typing import Any, Callable, Dict, List, Optional
from datetime import datetime
import logging
from requests_toolbelt import sessions
from urllib import parse as url_parse
from constants import API_URL, DEVICE_SETTINGS
from api import ConnectionCheck, ReplicationJob, Session, Source, Stream
from dotenv import load_dotenv

load_dotenv()


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def assert_status_hook(response, *args, **kwargs):
    return response.raise_for_status()


class StitchAPI:

    def __init__(self,
                 stitch_api_key: str,
                 stitch_client_id: int,
                 stitch_auth_user: str,
                 stitch_auth_password: str
                 ) -> None:
        self.stitch_api_key = stitch_api_key
        self.stitch_client_id = stitch_client_id

        self.client = sessions.BaseUrlSession(base_url=API_URL)
        self.client.hooks["response"] = [assert_status_hook]

        self.headers = {
                        'Accept': 'application/json',
                        'Origin': 'https://app.stitchdata.com',
                        'User-Agent': DEVICE_SETTINGS['user_agent'],
                        'Content-Type': 'application/json',
                        'Authorization': 'Bearer %s' % stitch_api_key
        }

        self._login(stitch_auth_user, stitch_auth_password)

    def _login(self, stitch_auth_user: str, stitch_auth_password: str) -> bool:

        # TODO use Session
        logger.debug('Authenticating internal API')
        data = '{{"email":"{user}","password":"{password}","remember-me":false}}'\
            .format(user=stitch_auth_user, password=stitch_auth_password)
        _ = self.client.post('/session', headers=self.headers, data=data)
        return True

    def _execute_request(self, api_call: Callable, *args, **kwargs) -> Any:
        logger.debug('Request {}'.format(api_call.__qualname__))
        send_request = api_call(*args, **kwargs)
        func = getattr(self.client, send_request.method)
        return func(send_request.endpoint,
                    headers=self.headers,
                    data=send_request.payload)

    def list_sources(self, include_deleted=False) -> List[Dict[str, Any]]:
        sources = self._execute_request(Source.list).json()
        if not include_deleted:
            sources = list(filter(lambda x: not x['deleted_at'],
                                  sources))
        return sources

    def get_source_from_name(self, source_name: str) -> Dict[str, Any]:
        assert source_name, 'source_name must be non-null'
        sources = [i for i in self.list_sources() if i['name'] == source_name]
        if not sources:
            raise ValueError('No matching source found for "{}"'.format(source_name))
        return sources

    def _list_streams(self, source_id) -> List[Dict[str, Any]]:
        return self._execute_request(Stream.list, source_id=source_id).json()

    def list_streams(self, source_name: str, selected_only: bool = False):
        source = self.get_source_from_name(source_name)

        sources = self._list_streams(source['id'])
        if selected_only:
            sources = list(filter(lambda x: x['selected'], sources))
        return sources

    def get_stream_from_name(self, source_name: str, stream_name: str) -> List[Dict[str, Any]]:
        assert all([source_name, stream_name]), "Must supply both stream and source names"
        source = self.get_source_from_name(source_name)
        source_id = source['id']
        streams = self.get_streams(source_id)
        matching_streams = [i for i in streams if i['stream_name'] == stream_name]
        if not matching_streams:
            raise ValueError("No matching stream found for {} in {}".format(stream_name,
                                                                            source_name))
        return matching_streams[0]

    def get_stream_schema_from_name(self, source_name: str, stream_name: str) -> Any:
        source = self.get_source_from_name(source_name)
        source_id = source['id']
        stream = self.get_stream_from_name(source_id, stream_name)
        stream_id = stream['stream_id']
        response = self._execute_request(Stream.get_schema, source_id=source_id,
                                         stream_id=stream_id)
        return response

    def _reset_stream(self, source_id: int, stream_id: int) -> Any:
        response = self._execute_request(Stream.reset, source_id=source_id, stream_id=stream_id,
                                         client_id=self.stitch_client_id)
        return response

    def reset_stream(self, source_name: str, stream_name: str) -> Any:
        source = self.get_source_from_name(source_name)
        source_id = source['id']
        stream = self.get_stream_from_name(source_id, stream_name)
        stream_id = stream['stream_id']
        response = self._reset_stream(source_id, stream_id)
        return response

    def _reset_integration(self, source_id: int) -> Any:
        response = self._execute_request(Source.reset, source_id=source_id,
                                         client_id=self.stitch_client_id)
        return response

    def reset_integration(self, source_name: str) -> Any:
        source = self.get_source_from_name(source_name)
        response = self._reset_integration(source['id'])
        return response

    def get_replication_schedule(self, source_name: str) -> Dict[str, Any]:
        source = self.get_source_from_name(source_name)

        return source['schedule']

    def set_replication_schedule(self, source_name: str,
                                 cron_expression: Optional[str] = None,
                                 frequency_in_minutes: Optional[str] = None) -> Any:
        """examples:
                   set_replication_schedule('airflow', cron_expression='0 */30 * * * ?')
                   set_replication_schedule('airflow', frequency_in_minutes=30)
        """
        assert(all([cron_expression or frequency_in_minutes,
                    not(cron_expression and frequency_in_minutes)]))

        if cron_expression:
            data = '{{"properties":{{"cron_expression":"{}"}}}}'.format(cron_expression)
        elif frequency_in_minutes:
            data = ('{{"properties":{{"frequency_in_minutes":"{}",'
                    '"cron_expression":null}}}}').format(frequency_in_minutes)
        else:
            raise Exception('must be cron or minute interval')
        source = self.get_source_from_name(source_name)
        response = self._execute_request(Source.update, source_id=source['id'],
                                         payload=data)
        return response

    def pause_source(self, source_name: str) -> Any:
        source = self.get_source_from_name(source_name)
        response = self._execute_request(Source.pause, source_id=source['id'])
        return response

    def unpause_source(self, source_name: str) -> Any:
        source = self.get_source_from_name(source_name)
        response = self._execute_request(Source.unpause, source_id=source['id'])
        return response

    def get_loads(self, source_name: str, stream_name: str, limit: int, offset: int,
                  time_range_start: datetime, time_range_end: datetime) -> Any:
        # TODO clean up and assert time ranges
        time_start = url_parse.quote(time_range_start.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z')
        time_end = url_parse.quote(time_range_end.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z')
        source = self.get_source_from_name(source_name)
        source_id = source['id']

        response = self._execute_request(Stream.get_load_data, source_id=source_id,
                                         stream_name=stream_name,
                                         time_range_start=time_start,
                                         time_range_end=time_end,
                                         limit=limit, offset=0, client_id=self.stitch_client_id)
        return response.json()

    def source_connection_check(self, source_name: str) -> Any:
        source = self.get_source_from_name(source_name)
        self._execute_request(ConnectionCheck.get, source_id=source['id'])

    def start_repliction(self, source_name: str) -> Any:
        source = self.get_source_from_name(source_name)
        self._execute_request(ReplicationJob.start, source_id=source['id'])

    def stop_replication(self, source_name: str) -> Any:
        source = self.get_source_from_name(source_name)
        self._execute_request(ReplicationJob.stop, source_id=source['id'])

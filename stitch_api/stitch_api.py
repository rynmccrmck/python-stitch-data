from typing import Any, Callable, Dict, List, Optional
from datetime import datetime
import functools
import logging
from collections import defaultdict
from requests_toolbelt import sessions
from urllib import parse as url_parse
from constants import API_URL, DEVICE_SETTINGS
from api import ConnectionCheck, ReplicationJob, Source, Stream
from dotenv import load_dotenv

load_dotenv()


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def assert_status_hook(response, *args, **kwargs):
    return response.raise_for_status()


def internal_login_required(func):
    @functools.wraps(func)
    def wrapper_client_provider(*args, **kwargs):
        self = args[0]
        if not self._logged_in_internal:
            self._login(self.stitch_auth_user, self.stitch_auth_password)
        value = func(*args, **kwargs)
        return value
    return wrapper_client_provider


def read_only(func):
    @functools.wraps(func)
    def wrapper_read_only(*args, **kwargs):
        kwargs.update(read_only=True)
        value = func(*args, **kwargs)
        return value
    return wrapper_read_only


class WriteBlacklist:

    def __init__(self, source_entries: List, stream_entries: Dict[str, List]) -> None:
        self.source_entries = source_entries
        self.stream_entries = stream_entries

    def verify_request(self, source_id: int, stream_id: int):
        if source_id in self.source_entries:
            raise ValueError('Source {} has been blacklisted from writes'.format(source_id))
        if stream_id in self.stream_entries:
            if stream_id in self.stream_entries[source_id]:
                raise ValueError('Stream {} ({}) has been blacklisted from writes'.format(stream_id,
                                                                                          source_id))


class StitchAPI:

    def __init__(self,
                 stitch_api_key: str,
                 stitch_client_id: int,
                 stitch_auth_user: str,
                 stitch_auth_password: str,
                 stitch_blacklist_sources: str = None,
                 ) -> None:
        self.stitch_api_key = stitch_api_key
        self.stitch_client_id = stitch_client_id
        self.stitch_auth_user = stitch_auth_user
        self.stitch_auth_password = stitch_auth_password

        self.client = sessions.BaseUrlSession(base_url=API_URL)
        self.client.hooks["response"] = [assert_status_hook]

        self.headers = {
                        'Accept': 'application/json',
                        'Origin': 'https://app.stitchdata.com',
                        'User-Agent': DEVICE_SETTINGS['user_agent'],
                        'Content-Type': 'application/json',
                        'Authorization': 'Bearer %s' % stitch_api_key
        }
        self._logged_in_internal = False
        self.write_blacklist = None
        self._parse_blacklist_config(stitch_blacklist_sources)

    def _parse_blacklist_config(self, config_string: str) -> None:
        if not config_string:
            self.write_blacklist = None
        entries = config_string.split(',')
        stream_entries = defaultdict(list)
        source_entries = []
        for entry in entries:
            source_name, stream_name = entry.split(".") + [None]
            source_id = self.get_source_from_name(source_name)['id']
            if stream_name:
                stream_id = self.get_stream_from_name(stream_name)['stream_id']
                stream_entries[source_id].append(stream_id)
            else:
                source_entries.append(source_id)
        if stream_entries or source_entries:
            self.write_blacklist =  WriteBlacklist(source_entries, stream_entries)

    def _login(self, stitch_auth_user: str, stitch_auth_password: str) -> bool:
        # TODO use Session
        logger.debug('Authenticating internal API')
        data = '{{"email":"{user}","password":"{password}","remember-me":false}}'\
            .format(user=stitch_auth_user, password=stitch_auth_password)
        _ = self.client.post('/session', headers=self.headers, data=data)
        return True

    def _execute_request(self, api_call: Callable, return_json: bool = False, *args, **kwargs) -> Any:
        logger.debug('Request {}'.format(api_call.__qualname__))
        if self.write_blacklist and not kwargs.get('read_only'):
            self.write_blacklist.verify_request(source_id=kwargs.get('source_id'),
                                                stream_id=kwargs.get('stream_id'))
        send_request = api_call(*args, **kwargs)
        func = getattr(self.client, send_request.method)
        response = func(send_request.endpoint,
                        headers=self.headers,
                        data=send_request.payload)
        if return_json:
            return response.json()
        return {'STATUS': response.status_code}

    @read_only
    def list_sources(self, include_deleted=False, *args, **kwargs) -> List[Dict[str, Any]]:
        sources = self._execute_request(Source.list, return_json=True, *args, **kwargs)
        if not include_deleted:
            sources = list(filter(lambda x: not x['deleted_at'],
                                  sources))
        return sources

    @read_only
    def get_source_from_name(self, source_name: str, *args, **kwargs) -> Dict[str, Any]:
        assert source_name, 'source_name must be non-null'
        sources = [i for i in self.list_sources() if i['name'] == source_name]
        if not sources:
            raise ValueError('No matching source found for "{}"'.format(source_name))
        return sources[0]

    @read_only
    def _list_streams(self, source_id, *args, **kwargs) -> List[Dict[str, Any]]:
        return self._execute_request(Stream.list, source_id=source_id, return_json=True, *args, **kwargs)

    @read_only
    def list_streams(self, source_name: str, selected_only: bool = False, *args, **kwargs):
        source = self.get_source_from_name(source_name)

        sources = self._list_streams(source['id'])
        if selected_only:
            sources = list(filter(lambda x: x['selected'], sources))
        return sources

    @read_only
    def get_stream_from_name(self, source_name: str, stream_name: str,
                             *args, **kwargs) -> List[Dict[str, Any]]:
        assert all([source_name, stream_name]), "Must supply both stream and source names"
        source = self.get_source_from_name(source_name)
        source_id = source['id']
        streams = self._list_streams(source_id)
        matching_streams = [i for i in streams if i['stream_name'] == stream_name]
        if not matching_streams:
            raise ValueError("No matching stream found for {} in {}".format(stream_name,
                                                                            source_name))
        return matching_streams[0]

    @read_only
    def get_stream_schema_from_name(self, source_name: str, stream_name: str,
                                    *args, **kwargs) -> Any:
        source = self.get_source_from_name(source_name)
        source_id = source['id']
        # TODO optional id
        stream = self.get_stream_from_name(source_name, stream_name)
        stream_id = stream['stream_id']
        response = self._execute_request(Stream.get_schema, source_id=source_id,
                                         stream_id=stream_id, return_json=True, *args, **kwargs)
        return response

    @internal_login_required
    def _reset_stream(self, source_id: int, stream_id: int, *args, **kwargs) -> Any:
        response = self._execute_request(Stream.reset, source_id=source_id, stream_id=stream_id,
                                         client_id=self.stitch_client_id, *args, **kwargs)
        return response

    def reset_stream(self, source_name: str, stream_name: str, *args, **kwargs) -> Any:
        source = self.get_source_from_name(source_name)
        source_id = source['id']
        # TODO optional id
        stream = self.get_stream_from_name(source_name, stream_name)
        stream_id = stream['stream_id']
        response = self._reset_stream(source_id, stream_id)
        return response

    @internal_login_required
    def _reset_integration(self, source_id: int, *args, **kwargs) -> Any:
        response = self._execute_request(Source.reset, source_id=source_id,
                                         client_id=self.stitch_client_id, *args, **kwargs)
        return response

    def reset_integration(self, source_name: str, *args, **kwargs) -> Any:
        source = self.get_source_from_name(source_name)
        response = self._reset_integration(source['id'])
        return response

    @read_only
    def get_replication_schedule(self, source_name: str, *args, **kwargs) -> Dict[str, Any]:
        source = self.get_source_from_name(source_name, return_json=True)

        return source['schedule']

    def set_replication_schedule(self, source_name: str,
                                 cron_expression: Optional[str] = None,
                                 frequency_in_minutes: Optional[str] = None, *args, **kwargs) -> Any:
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
                                         payload=data, *args, **kwargs)
        return response

    def pause_source(self, source_name: str, *args, **kwargs) -> Any:
        source = self.get_source_from_name(source_name)
        response = self._execute_request(Source.pause, source_id=source['id'], *args, **kwargs)
        return response

    def unpause_source(self, source_name: str, *args, **kwargs) -> Any:
        source = self.get_source_from_name(source_name)
        response = self._execute_request(Source.unpause, source_id=source['id'], *args, **kwargs)
        return response

    @read_only
    @internal_login_required
    def get_loads(self, source_name: str, stream_name: str, limit: int, offset: int,
                  time_range_start: datetime, time_range_end: datetime, *args, **kwargs) -> Any:
        # TODO clean up and assert time ranges
        time_start = url_parse.quote(time_range_start.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z')
        time_end = url_parse.quote(time_range_end.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z')
        source = self.get_source_from_name(source_name)
        source_id = source['id']

        response = self._execute_request(Stream.get_load_data, source_id=source_id,
                                         stream_name=stream_name,
                                         time_range_start=time_start,
                                         time_range_end=time_end,
                                         limit=limit, offset=0, client_id=self.stitch_client_id,
                                         return_json=True, *args, **kwargs)
        return response

    @read_only
    def source_connection_check(self, source_name: str, *args, **kwargs) -> Any:
        source = self.get_source_from_name(source_name)
        self._execute_request(ConnectionCheck.get, source_id=source['id'], return_json=True,
                              *args, **kwargs)

    def start_repliction(self, source_name: str, *args, **kwargs) -> Any:
        source = self.get_source_from_name(source_name)
        self._execute_request(ReplicationJob.start, source_id=source['id'], *args, **kwargs)

    def stop_replication(self, source_name: str, *args, **kwargs) -> Any:
        source = self.get_source_from_name(source_name)
        self._execute_request(ReplicationJob.stop, source_id=source['id'], *args, **kwargs)

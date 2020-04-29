from typing import Any, Callable, Dict, List, Optional
from datetime import datetime, timedelta
import functools
import logging
from collections import defaultdict
from requests_toolbelt import sessions
from stitch_api import constants
from stitch_api import api
from dotenv import load_dotenv
from .constants import MAX_REPORT_DAYS

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

        self.client = sessions.BaseUrlSession(base_url=constants.API_URL)
        self.client.hooks["response"] = [assert_status_hook]

        self.headers = {
                        'Accept': 'application/json',
                        'Origin': 'https://app.stitchdata.com',
                        'User-Agent': constants.DEVICE_SETTINGS['user_agent'],
                        'Content-Type': 'application/json',
                        'Authorization': 'Bearer %s' % stitch_api_key
        }
        self._logged_in_internal = False
        self.write_blacklist = None
        self._parse_blacklist_config(stitch_blacklist_sources)

    def _parse_blacklist_config(self, config_string: str) -> bool:
        if not config_string:
            self.write_blacklist = None
            return False
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
            self.write_blacklist = WriteBlacklist(source_entries, stream_entries)
        return True

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
        sources = self._execute_request(api.Source.list, return_json=True, *args, **kwargs)
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
        return self._execute_request(api.Stream.list, source_id=source_id, return_json=True,
                                     *args, **kwargs)

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
    def get_stream_schema(self, source_id: str, stream_id: str,
                          *args, **kwargs) -> Any:
        response = self._execute_request(api.Stream.get_schema, source_id=source_id,
                                         stream_id=stream_id, return_json=True, *args, **kwargs)
        return response

    @read_only
    def get_stream_schema_from_name(self, source_name: str, stream_name: str,
                                    *args, **kwargs) -> Any:
        source = self.get_source_from_name(source_name)
        source_id = source['id']
        # TODO optional id
        stream = self.get_stream_from_name(source_name, stream_name)
        stream_id = stream['stream_id']
        response = self._execute_request(api.Stream.get_schema, source_id=source_id,
                                         stream_id=stream_id, return_json=True, *args, **kwargs)
        return response

    @internal_login_required
    def _reset_stream(self, source_id: int, stream_id: int, *args, **kwargs) -> Any:
        response = self._execute_request(api.Stream.reset, source_id=source_id, stream_id=stream_id,
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
        response = self._execute_request(api.Source.reset, source_id=source_id,
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
            data = {"properties": {"cron_expression": cron_expression}}
        elif frequency_in_minutes:
            data = {"properties": {"frequency_in_minutes": str(frequency_in_minutes),
                    "cron_expression": None}}
        else:
            raise Exception('must be cron or minute interval')

        source = self.get_source_from_name(source_name)
        response = self._execute_request(api.Source.update, source_id=source['id'],
                                         payload=data, *args, **kwargs)
        return response

    def pause_source(self, source_name: str, *args, **kwargs) -> Any:
        source = self.get_source_from_name(source_name)
        response = self._execute_request(api.Source.pause, source_id=source['id'], *args, **kwargs)
        return response

    def unpause_source(self, source_name: str, *args, **kwargs) -> Any:
        source = self.get_source_from_name(source_name)
        response = self._execute_request(api.Source.unpause, source_id=source['id'], *args, **kwargs)
        return response

    @classmethod
    def adjust_date(cls, raw_datetime: datetime):
        earliest_date = datetime.now() - timedelta(days=MAX_REPORT_DAYS)
        if raw_datetime < earliest_date:
            logger.info("Stitch history is finite, changing start_date to {}".format(earliest_date))
            return earliest_date
        return raw_datetime

    def get_multi_day_reports(self, source_id: int, stream_name: str,
                              start_datetime: datetime, end_datetime: datetime):
        """
        Chunks load reports into days
        """
        delta = end_datetime - start_datetime
        date_list = [start_datetime + timedelta(days=x) for x in range(delta.days + 1)]
        if end_datetime > date_list[-1]:
            date_list.append(end_datetime)
        else:
            date_list[-1] = end_datetime
        reports = []
        for start, end in zip(date_list, date_list[1:]):
            report = self.get_loads('', stream_name=stream_name, limit=100, offset=0,
                                    time_range_start=start, time_range_end=end,
                                    source_id=source_id)
            reports.extend(report['batches'])
        return reports

    def get_stream_load_reports(self, source_id: int, stream_name: str,
                                start_datetime: datetime, end_datetime: datetime):
        # stitch has limited history
        start_datetime = self.adjust_date(start_datetime)
        end_datetime = self.adjust_date(end_datetime)
        delta = end_datetime - start_datetime

        # required as stitch internal API limits request range
        if delta.days > 0:
            reports = self.get_multi_day_reports(source_id, stream_name, start_datetime,
                                                 end_datetime)
        else:
            reports = self.get_loads('', stream_name=stream_name, limit=100, offset=0,
                                     time_range_start=start_datetime, time_range_end=end_datetime,
                                     source_id=source_id)['batches']
        return reports

    def get_source_load_reports(self, source_id: int,
                                start_datetime: datetime, end_datetime: datetime,
                                selected_only: bool = False):
        reports = []
        streams = self._list_streams(source_id=source_id)
        for stream in streams:
            if selected_only and not stream['selected']:
                continue
            report = self.get_stream_load_reports(source_id=source_id,
                                                  stream_name=stream['stream_name'],
                                                  start_datetime=start_datetime,
                                                  end_datetime=end_datetime)
            reports.extend(report)
        return reports

    @read_only
    @internal_login_required
    def get_loads(self, source_name: str, stream_name: str, limit: int, offset: int,
                  time_range_start: datetime, time_range_end: datetime, source_id: int = None,
                  *args, **kwargs) -> Any:

        if not source_id:
            source = self.get_source_from_name(source_name)
            source_id = source['id']

        # TODO clean up and assert time ranges
        time_start = time_range_start.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        time_end = time_range_end.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

        response = self._execute_request(api.Stream.get_load_data, source_id=source_id,
                                         stream_name=stream_name,
                                         start_iso=time_start,
                                         end_iso=time_end,
                                         limit=limit, offset=offset, client_id=self.stitch_client_id,
                                         return_json=True, *args, **kwargs)
        return response

    @read_only
    @internal_login_required
    def get_extractions(self, time_range_start: datetime, time_range_end: datetime,
                        source_id: Optional[int] = None, source_name: Optional[str] = None,
                        *args, **kwargs) -> Any:

        if not source_id:
            source = self.get_source_from_name(source_name)
            source_id = source['id']

        # TODO clean up and assert time ranges
        time_start = time_range_start.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        time_end = time_range_end.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

        response = self._execute_request(api.Source.get_extraction_data,
                                         source_id=source_id,
                                         start_iso=time_start,
                                         end_iso=time_end,
                                         client_id=self.stitch_client_id,
                                         return_json=True, *args, **kwargs)
        return response

    @read_only
    def source_connection_check(self, source_name: str, *args, **kwargs) -> Any:
        source = self.get_source_from_name(source_name)
        self._execute_request(api.ConnectionCheck.get, source_id=source['id'], return_json=True,
                              *args, **kwargs)

    def start_repliction(self, source_name: str, *args, **kwargs) -> Any:
        source = self.get_source_from_name(source_name)
        self._execute_request(api.ReplicationJob.start, source_id=source['id'], *args, **kwargs)

    def stop_replication(self, source_name: str, *args, **kwargs) -> Any:
        source = self.get_source_from_name(source_name)
        self._execute_request(api.ReplicationJob.stop, source_id=source['id'], *args, **kwargs)

    @internal_login_required
    def get_source_daily_report(self, source_name: str, *args, **kwargs) -> Any:
        source = self.get_source_from_name(source_name)
        daily_stats = self._execute_request(api.Source.daily_report, client_id=self.stitch_client_id,
                                            return_json=True, *args, **kwargs)
        return [i for i in daily_stats['stats'] if i['connection_id'] == source['id']]

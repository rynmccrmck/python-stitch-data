from collections import namedtuple
from datetime import datetime
from typing import Optional
from urllib import parse as url_parse
from .common import BaseStitchApi


class Source(BaseStitchApi):

    @classmethod
    def reset(cls, source_id, client_id=None, *args, **kwargs) -> namedtuple:
        url = ('/menagerie/public/v1/clients/{client_id}/connections/'
               '{source_id}/state').format(client_id=client_id,
                                           source_id=source_id)
        return cls.send_request(url, method='delete')

    @classmethod
    def daily_report(cls, client_id=None, *args, **kwargs) -> namedtuple:
        url = '/clients/{client_id}/stats/daily'.format(client_id=client_id)
        return cls.send_request(url, method='get')


class Stream(BaseStitchApi):

    @classmethod
    def reset(cls, source_id: int, stream_id: int, client_id: Optional[int] = None, *args, **kwargs) -> namedtuple:
        url = ('/menagerie/public/v1/clients/{client_id}/connections/'
               '{source_id}/bookmark/streams/{stream_id}').format(client_id=client_id,
                                                                  source_id=source_id,
                                                                  stream_id=stream_id)
        return cls.send_request(url, method='delete')

    @classmethod
    def get_load_data(cls, source_id: int, stream_name: str,
                      start_iso: str, end_iso: str,
                      limit: int = 100, offset: int = 0,
                      client_id: Optional[int] = None, *args, **kwargs) -> namedtuple:
        time_start = url_parse.quote(start_iso)
        time_end = url_parse.quote(end_iso)
        url = ("/clients/{client_id}/connections/{source_id}/loading-reports/tables/{stream_name}?"
               "limit={limit}&offset={offset}&time_range_end={time_range_end}"
               "&time_range_start={time_range_start}")
        url = url.format(client_id=client_id, source_id=source_id,
                         stream_name=stream_name,
                         limit=limit, offset=offset,
                         time_range_start=time_start, time_range_end=time_end)
        return cls.send_request(url, method='get')

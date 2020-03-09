from datetime import datetime
import json
from collections import namedtuple
from typing import Dict, Any
from .common import BaseStitchApi


def _encode_values(v):
    if v == 'null':
        return None
    return v


def create_filtered_payload(arg_dict) -> Dict[str, Any]:
    return {k: _encode_values(v) for k, v in arg_dict.items() if v is not None and k != 'cls'}


class Session(BaseStitchApi):

    @classmethod
    def create(cls, *args, **kwargs) -> namedtuple:
        return cls.send_request('/v3/sessions/ephemeral', method='post')


class Source(BaseStitchApi):

    @classmethod
    def create(cls, display_name: str, source_type: str,
               properties: Dict[str, Any] = None, *args, **kwargs) -> namedtuple:
        assert all([display_name, source_type])
        payload = create_filtered_payload(locals())
        return cls.send_request('/v4/sources', method='post', payload=json.dumps(payload))

    @classmethod
    def update(cls, source_id: int, payload: Dict[str, Any], *args, **kwargs) -> namedtuple:
        assert source_id
        # necessary as null values are potentially destructive
        payload = create_filtered_payload(payload)
        return cls.send_request('/v4/sources/{}'.format(source_id), method='put',
                                payload=json.dumps(payload))

    @classmethod
    def pause(cls, source_id: int, *args, **kwargs) -> namedtuple:
        assert source_id
        payload = {
                   'paused_at': datetime.now().isoformat(),
                   }
        return cls.update(source_id, payload=payload)

    @classmethod
    def unpause(cls, source_id: int, *args, **kwargs) -> namedtuple:
        assert source_id
        payload = {
                   'paused_at': 'null',
                   }
        return cls.update(source_id, payload=payload)

    @classmethod
    def list(cls, *args, **kwargs) -> namedtuple:
        return cls.send_request('/v4/sources', method='get')

    @classmethod
    def delete(cls, source_id: int, *args, **kwargs) -> namedtuple:
        assert source_id
        return cls.send_request('/v4/sources/{}'.format(source_id), method='delete')

    @classmethod
    def get(cls, source_id: int, *args, **kwargs) -> namedtuple:
        assert source_id
        return cls.send_request('/v4/sources/{}'.format(source_id), method='get')


class ConnectionCheck(BaseStitchApi):
    @classmethod
    def get(cls, source_id: int, *args, **kwargs) -> namedtuple:
        assert source_id
        return cls.send_request('/v4/sources/{}/last-connection-check'.format(source_id),
                                method='get')


class Stream(BaseStitchApi):
    @classmethod
    def list(cls, source_id: int, *args, **kwargs) -> namedtuple:
        assert source_id
        return cls.send_request('/v4/sources/{}/streams'.format(source_id), method='get')

    @classmethod
    def get_schema(cls, source_id: int, stream_id: int, *args, **kwargs) -> namedtuple:
        assert all([source_id, stream_id])
        return cls.send_request('/v4/sources/{0}/streams/{1}'.format(source_id, stream_id),
                                method='get')

    @classmethod
    def update_metadata(cls, source_id: int, *args, **kwargs) -> namedtuple:
        assert source_id
        raise NotImplementedError


class ReplicationJob(BaseStitchApi):

    @classmethod
    def start(cls, source_id: int, *args, **kwargs) -> namedtuple:
        assert source_id
        return cls.send_request('/v4/sources/{}/sync'.format(source_id), method='post')

    @classmethod
    def stop(cls, source_id: int, *args, **kwargs) -> namedtuple:
        assert source_id
        return cls.send_requests('/v4/sources/{}/sync'.format(source_id), method='delete')

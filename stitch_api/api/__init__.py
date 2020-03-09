from .public import Session, Source as PublicSource, Stream as PublicStream,\
                     ConnectionCheck, ReplicationJob  # noqa: F401
from .internal import Source as PrivateSource, Stream as PrivateStream


class Source(PublicSource, PrivateSource):
    pass


class Stream(PublicStream, PrivateStream):
    pass

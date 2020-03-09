#!/usr/bin/env python3
import click
import os
from stitch_api import StitchAPI
import functools


STITCH_API_KEY = os.getenv('STITCH_API_KEY')
STITCH_CLIENT_ID = os.getenv('STITCH_CLIENT_ID')
STITCH_AUTH_USER = os.getenv('STITCH_AUTH_USER')
STITCH_AUTH_PASSWORD = os.getenv('STITCH_AUTH_PASSWORD')
STITCH_BLACKILST_SOURCES = os.getenv('STITCH_BLACKILST_SOURCES')


def provide_client(func):
    @functools.wraps(func)
    def wrapper_client_provider(*args, **kwargs):
        stitch_api = StitchAPI(STITCH_API_KEY,
                               STITCH_CLIENT_ID,
                               STITCH_AUTH_USER,
                               STITCH_AUTH_PASSWORD,
                               STITCH_BLACKILST_SOURCES)
        kwargs.update(stitch_api=stitch_api)
        value = func(*args, **kwargs)
        return value
    return wrapper_client_provider


@click.group()
def cli1():
    pass


@cli1.command()
@click.option('--include-deleted', default=False, help='Include deleted sources.')
@provide_client
def list_sources(include_deleted, stitch_api=None):
    """List all sources"""
    sources = stitch_api.list_sources(include_deleted=include_deleted)
    print(sources)


@cli1.command()
@click.option('--source', help='Source name')
@click.option('--selected_only', default=False, help='Return only selected streams')
@click.option('--fields', default=None, help='Comma separated list')
@provide_client
def list_streams(source, selected_only, fields=None, stitch_api=None):
    """List streams in a source"""
    streams = stitch_api.list_streams(source, selected_only)
    if fields:
        for stream in streams:
            output = {'stream_id': stream['stream_id'], 'stream_name': stream['stream_name']}
            for f in fields.split(','):
                output[f] = stream[f]
            print(output)
    else:
        print(streams)


@cli1.command()
@click.option('--source', help='Source name')
@provide_client
def get_source(source, stitch_api=None):
    """Get source info"""
    source = stitch_api.get_source_from_name(source)
    print(source)


@cli1.command()
@click.option('--source', help='Source name')
@click.option('--stream', help='Stream name')
@provide_client
def get_stream(source, stream, stitch_api=None):
    """Get stream info"""
    stream = stitch_api.get_stream_from_name(source, stream)
    print(stream)


@cli1.command()
@click.option('--source', help='Source name')
@click.option('--stream', help='Stream name')
@provide_client
def get_stream_schema(source, stream, stitch_api=None):
    """Get stream schema"""
    stream = stitch_api.get_stream_schema_from_name(source, stream)
    print(stream)


@cli1.command()
@click.option('--source', help='Source name')
@click.option('--stream', help='Stream name')
@provide_client
def reset_stream(source, stream, stitch_api=None):
    """Reset a stream"""
    response = stitch_api.reset_stream(source, stream)
    print(response)


@cli1.command()
@click.option('--source', help='Source name')
@provide_client
def reset_source(source, stitch_api=None):
    """Reset a source"""
    response = stitch_api.reset_integration(source)
    print(response)


@cli1.command()
@click.option('--source', help='Source name')
@provide_client
def get_schedule(source, stitch_api=None):
    """Get source replication schedule"""
    response = stitch_api.get_replication_schedule(source)
    print(response)


@cli1.command()
@click.option('--source', help='Source name')
@provide_client
def pause_source(source, stitch_api=None):
    """Pause a source"""
    response = stitch_api.pause_source(source)
    print(response)


@cli1.command()
@click.option('--source', help='Source name')
@provide_client
def unpause_source(source, stitch_api=None):
    """Unpause a source"""
    response = stitch_api.unpause_source(source)
    print(response)


@cli1.command()
@click.option('--source', help='Source name')
@provide_client
def connection_check(source, stitch_api=None):
    """Get last source connection check"""
    response = stitch_api.source_connection_check(source)
    print(response)


@cli1.command()
@click.option('--source', help='Source name')
@provide_client
def start_replication(source, stitch_api=None):
    """Kick off replication job"""
    _ = stitch_api.start_repliction(source)


@cli1.command()
@click.option('--source', help='Source name')
@provide_client
def stop_replication(source, stitch_api=None):
    """Stop replication job"""
    _ = stitch_api.stop_repliction(source)


cli = click.CommandCollection(sources=[cli1])
if __name__ == '__main__':
    cli()

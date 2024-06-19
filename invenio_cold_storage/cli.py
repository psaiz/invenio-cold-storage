import json
import sys
from functools import wraps
from math import log2
import click
from flask import current_app
from flask.cli import with_appcontext

from .manager import ColdStorageManager

argument_record = click.argument("record", nargs=-1, required=True, metavar="RECORD")

option_file = click.option(
    "-f", "--file", multiple=True, default=[], metavar="FILE", help="File(s)."
)

option_register = click.option('--register/--no-register',
                               help="If the file already exists at the destination, with the same file and checksum, import it without issuing the transfer"
)

option_debug = click.option('--debug/--no-debug', default=False)
option_exists = click.option('--check-exists/--no-check-exists', default=True)

option_limit = click.option('--limit', default=False)


# From https://stackoverflow.com/questions/1094841/get-a-human-readable-version-of-a-file-size
def file_size(size):
    """Convert a size in bytes to a human readable format"""
    _suffixes = ['bytes', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB']
    order = int(log2(size) / 10) if size else 0
    return '{:.4g} {}'.format(size / (1 << (order * 10)), _suffixes[order])
@click.group()
def cold():
    """Manage the cold interface."""


@cold.command()
@with_appcontext
@argument_record
@option_file
@option_register
@option_debug
@option_limit
@option_exists
def archive(record, file, register, debug, limit, check_exists):
    """Move a record to cold"""
    click.secho(f"Moving the record {record} to cold", fg="green")
    m = ColdStorageManager(current_app, debug)
    counter = 0
    transfers = 0
    for r in record:
        t = m.archive(r, file, register, check_exists)
        transfers += len(t)
        counter += 1
        click.secho(f"Record {r} moved. Entry {counter} out of {len(record)} done. {transfers} issued so far", fg="green")


@cold.command()
@with_appcontext
@argument_record
@option_file
def stage(record, file):
    """Move a record from cold"""
    click.secho(f"Moving {record} from cold", fg="green")
    m = ColdStorageManager(current_app)
    m.stage(record, file)


@cold.command()
@with_appcontext
def settings():
    """Display the list of configured cold endpoints."""
    m = ColdStorageManager(current_app)
    click.secho(f"The cold storage interface will store in: {m.settings()}")


@cold.command()
@with_appcontext
@argument_record
@option_file
@option_debug
def list(record, file, debug):
    """Print the urls for an entry.

    By default, it prints the urls for all the files of the entry.
    """
    m = ColdStorageManager(current_app, debug)
    stats ={'files': 0, 'hot':0, 'cold':0, "size":0, "size_hot":0, "size_cold":0, "errors":[]}
    for r in record:
        info = m.list(r, file)
        if debug:
            print("Printing debug info", info)
        if not info:
            click.secho(f"The record {r} does not exist!")
            stats["errors"] +=[r]
            continue
        click.secho(f"The files referenced in '{r}' are:", fg="green")

        for f in info:
            stats["files"] += 1
            stats["size"] += f['size']
            if not "tags" in f or not "hot_deleted" in f["tags"]:
                print(f"    * Hot copy: {f['uri']}")
                stats["hot"] += 1
                stats["size_hot"] += f['size']
            if "tags" in f and "uri_cold" in f["tags"]:
                print(f"    * Cold copy: {f['tags']['uri_cold']}")
                stats["cold"] += 1
                stats["size_cold"] += f['size']

    click.secho(f"Summary: {stats['files']} files ({file_size(stats['size'])}), with {stats['hot']} hot copies ({file_size(stats['size_hot'])}) and {stats['cold']} cold copies ({file_size(stats['size_cold'])}) ", fg="green")
    if stats["errors"]:
        click.secho(f"The following records have issues: {stats['errors']}", fg="red")
        return -1

@cold.command()
@with_appcontext
@argument_record
@option_file
def clear_hot(record, file):
    """Delete the hot copy of a file that has a cold copy."""
    m = ColdStorageManager(current_app)
    m.clear_hot(record, file)


@cold.command()
@with_appcontext
@option_debug
def  check_transfers(debug):
    m = ColdStorageManager(current_app, debug)
    return m.check_current_transfers()
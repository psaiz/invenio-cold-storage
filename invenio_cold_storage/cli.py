import json
import sys
from functools import wraps

import click
from flask import current_app
from flask.cli import with_appcontext

from .manager import ColdStorageManager

argument_record = click.argument("record", nargs=1, required=True, metavar="RECORD")

option_file = click.option(
    "-f", "--file", multiple=True, default=[], metavar="FILE", help="File(s)."
)


@click.group()
def cold():
    """Manage the cold interface."""


@cold.command()
@with_appcontext
@argument_record
@option_file
def archive(record, file):
    """Move a record to cold"""
    click.secho(f"Moving the record {record} to cold", fg="green")
    m = ColdStorageManager(current_app)
    m.archive(record, file)


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
def list(record, file):
    """Print the urls for an entry.

    By default, it prints the urls for all the files of the entry.
    """
    m = ColdStorageManager(current_app)
    info = m.list(record, file)
    for f in info:
        print(f"  * File: {f['key']}:")
        if "uri" in f:
            print(f"    * Hot copy: {f['uri']}")
        if "uri_cold" in f:
            print(f"    * Cold copy: {f['uri_cold']}")
        if "dataset" in f:
            print(f"    * This entry is a dataset, which contains the following files:")
            for sub_file in f["dataset"]:
                print(f"      * {sub_file['filename']}")
                if "uri" in sub_file:
                    print(f"        * Hot copy: {sub_file['uri']}")
                if "uri_cold" in sub_file:
                    print(f"        * Cold copy: {sub_file['uri_cold']}")


@cold.command()
@with_appcontext
def check_current_transfers():
    m = ColdStorageManager(current_app)
    m.check_current_transfers()


@cold.command()
@with_appcontext
@argument_record
@option_file
def clear_hot(record, file):
    """Delete the hot copy of a file that has a cold copy."""
    m = ColdStorageManager(current_app)
    m.clear_hot(record, file)

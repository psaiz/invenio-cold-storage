# -*- coding: utf-8 -*-
#
# Copyright (C) 2020-2021 CERN.
# Copyright (C) 2020-2021 Northwestern University.
#
# Invenio-Records-Resources is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.

"""Example service."""

from invenio_records_resources.services import (
    FileServiceConfig,
    RecordServiceConfig,
    SearchOptions,
)
from invenio_records_resources.services.files.links import FileLink
from invenio_records_resources.services.records.components import FilesComponent
from invenio_records_resources.services.records.config import SearchOptions
from invenio_records_resources.services.records.facets import (
    NestedTermsFacet,
    TermsFacet,
)
from invenio_records_resources.services.records.links import (
    RecordLink,
    pagination_links,
)
from invenio_records_resources.services.records.params.querystr import (
    SuggestQueryParser,
)

from .api import Record
from .permissions import PermissionPolicy
from .schemas import RecordSchema


class MockSearchOptions(SearchOptions):
    """Mock module search options."""

    facets = {
        "type": NestedTermsFacet(
            field="metadata.type.type",
            subfield="metadata.type.subtype",
            splitchar="**",
            label="Type",
        ),
        "subject": TermsFacet(
            field="metadata.subject",
            label="Subject",
        ),
    }

    suggest_parser_cls = SuggestQueryParser.factory(
        fields=["metadata.title", "metadata.title._2gram", "metadata.title._3gram"]
    )


class ServiceConfig(RecordServiceConfig):
    """Mock service configuration.

    Needs both configs, with File overwritting the record ones.
    """

    service_id = "mock-records"
    permission_policy_cls = PermissionPolicy
    record_cls = Record
    schema = RecordSchema
    search = MockSearchOptions

    links_item = {
        "self": RecordLink("{+api}/mocks/{id}"),
        "files": RecordLink("{+api}/mocks/{id}/files"),
        "files-archive": RecordLink("{+api}/mocks/{id}/files-archive"),
    }

    links_search = pagination_links("{+api}/mocks{?args*}")

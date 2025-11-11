"""
UmaDB Python Client

A Python client for UmaDB event store using Rust bindings via PyO3.
"""


from umadb._umadb import (
    Client,
    Event,
    SequencedEvent,
    Query,
    QueryItem,
    AppendCondition,
)

__version__ = "0.1.5"

__all__ = [
    "Client",
    "Event",
    "SequencedEvent",
    "Query",
    "QueryItem",
    "AppendCondition",
]

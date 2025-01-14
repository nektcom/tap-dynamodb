"""Stream type classes for tap-dynamodb."""

from __future__ import annotations

import sys
import typing as t

from custom_logger import user_logger
from singer_sdk.streams import Stream

if t.TYPE_CHECKING:
    from collections.abc import Iterable

    from singer_sdk.helpers.types import Context
    from singer_sdk.tap_base import Tap

    from tap_dynamodb.dynamodb_connector import DynamoDbConnector


class TableStream(Stream):
    """Stream class for TableStream streams."""

    def __init__(
        self,
        tap: Tap,
        name: str,
        dynamodb_conn: DynamoDbConnector,
        infer_schema_sample_size,
    ):
        """Initialize a new TableStream object.

        Args:
            tap: The parent tap object.
            name: The name of the stream.
            dynamodb_conn: The DynamoDbConnector object.
            infer_schema_sample_size: The amount of records to sample when
                inferring the schema.
        """
        self._dynamodb_conn: DynamoDbConnector = dynamodb_conn
        self._table_name: str = name
        self._schema: dict = {}
        self._infer_schema_sample_size = infer_schema_sample_size
        self._table_scan_kwargs: dict = tap.config.get("table_scan_kwargs", {}).get(name, {})
        if tap.input_catalog:
            catalog_entry = tap.input_catalog.get(name)
            if catalog_entry:
                super().__init__(
                    name=name,
                    tap=tap,
                    schema=catalog_entry.to_dict().get("schema"),
                )
            else:
                user_logger.error(
                    f"Catalog provided with selected table '{name}' missing. "
                    "Either add the table to the catalog or remove it from the config."
                )
                sys.exit(1)
        else:
            super().__init__(name=name, tap=tap)

    def get_records(self, context: Context | None) -> Iterable[dict]:
        """Generate records from the stream."""
        for batch in self._dynamodb_conn.get_items_iter(
            self._table_name,
            self._table_scan_kwargs,
        ):
            yield from batch

    @property
    def schema(self) -> dict:
        """Dynamically detect the json schema for the stream.

        This is evaluated prior to any records being retrieved.

        Returns:
            dict
        """
        # TODO: SDC columns
        if not self._schema:
            self._schema = self._dynamodb_conn.get_table_json_schema(
                self._table_name,
                self._infer_schema_sample_size,
                self._table_scan_kwargs,
            )
            self._primary_keys = self._dynamodb_conn.get_table_key_properties(self._table_name)
        return self._schema

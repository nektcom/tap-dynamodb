"""Stream type classes for tap-dynamodb."""

from __future__ import annotations

import hashlib
import json
import sys
import typing as t

from boto3.dynamodb.types import TypeDeserializer
from nekt_singer_sdk.custom_logger import user_logger
from nekt_singer_sdk.streams import Stream
from singer_sdk import typing as th

if t.TYPE_CHECKING:
    from collections.abc import Iterable

    from nekt_singer_sdk.helpers.types import Context
    from nekt_singer_sdk.tap_base import Tap

    from tap_dynamodb.dynamodb_connector import DynamoDbConnector


class TableStream(Stream):
    """Stream class for TableStream streams."""

    user_defined_replication_key = None

    def __init__(
        self,
        tap: Tap,
        name: str,
        dynamodb_conn: DynamoDbConnector,
        infer_schema_sample_size: int,
        replication_key: str | None,
        replication_method: str,
    ):
        """Initialize a new TableStream object.

        Args:
            tap: The parent tap object.
            name: The name of the stream.
            dynamodb_conn: The DynamoDbConnector object.
            infer_schema_sample_size: The amount of records to sample when
                inferring the schema.
            replication_key: The key to use for incremental replication.
            replication_method: The method to use for incremental replication.
        """
        self.user_defined_replication_key = replication_key
        self.user_defined_replication_method = replication_method
        self.deserializer = TypeDeserializer()

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
                    f"Catalog provided with selected table '{name}' missing. Either add the table to the catalog or remove it from the config."
                )
                sys.exit(1)
        else:
            super().__init__(name=name, tap=tap)

    @property
    def schema(self) -> dict:
        """Dynamically detect the json schema for the stream.

        This is evaluated prior to any records being retrieved.

        Returns:
            dict
        """
        if not self._schema:
            if self.config.get("extraction_mode") == "infer_schema":
                self._schema = self._dynamodb_conn.get_table_json_schema(
                    self._table_name,
                    self._infer_schema_sample_size,
                    self._table_scan_kwargs,
                )
                # Coerce the replication key to a datetime if it's a string
                if (
                    self.user_defined_replication_key
                    and self.user_defined_replication_key in self._schema["properties"]
                    and self._schema["properties"][self.user_defined_replication_key]["type"] == "string"
                ):
                    self._schema["properties"][self.user_defined_replication_key]["format"] = "date-time"

                self._primary_keys = self._dynamodb_conn.get_table_key_properties(self._table_name)
            elif self.config.get("extraction_mode") == "envelope":
                envelope_schema = th.PropertiesList(
                    th.Property("_hash_id", th.StringType),
                    th.Property("document", th.StringType),
                )

                if self.user_defined_replication_key:
                    envelope_schema.append(th.Property(self.user_defined_replication_key, th.DateTimeType))

                self._primary_keys = ["_hash_id"]
                self._schema = envelope_schema.to_dict()

            self._replication_key = self.user_defined_replication_key
            self._replication_method = self.user_defined_replication_method

            user_logger.info(f"[{self._table_name}] Inferred schema: {self._schema}")
        return self._schema

    def get_records(self, context: Context | None) -> Iterable[dict]:
        """Generate records from the stream."""
        total_records = 0
        if self._replication_key and self.get_starting_replication_key_value(context):
            user_logger.info(
                f"Using replication key: {self.replication_key} with starting value: {self.get_starting_replication_key_value(context)}"
            )
            self._table_scan_kwargs["FilterExpression"] = f"#incremental_filter > :incremental_value"
            self._table_scan_kwargs["ExpressionAttributeNames"] = {"#incremental_filter": self.replication_key}
            self._table_scan_kwargs["ExpressionAttributeValues"] = {
                ":incremental_value": self.get_starting_replication_key_value(context)
            }

        try:
            for batch in self._dynamodb_conn.get_items_iter(
                self._table_name,
                self._table_scan_kwargs,
            ):
                user_logger.info(f"Processing batch of {len(batch)} records for table {self._table_name}")
                total_records += len(batch)
                for record in batch:
                    try:
                        yield self.process_record(record)
                    except Exception as e:
                        user_logger.error(f"Error processing individual record: {record}. Error details: {str(e)}")
                        sys.exit(1)
            user_logger.info(f"Total records processed for table {self._table_name}: {total_records}")
        except Exception as e:
            user_logger.error(f"Error getting records for table {self._table_name}. Error details: {str(e)}")
            user_logger.error(f"Table scan kwargs: {self._table_scan_kwargs}")
            sys.exit(1)

    def process_record(self, record: dict) -> dict:
        if self.config.get("extraction_mode") == "envelope":
            processed_record = {
                "_hash_id": self.generate_hash(
                    [record.get(key) for key in self._primary_keys if record.get(key) is not None]
                ),
                "document": record,
            }

            if self.replication_key:
                processed_record[self.replication_key] = record.get(self.replication_key)
        else:
            processed_record = record

        return processed_record

    def generate_hash(self, primary_keys: list[str]) -> str:
        combined_string = "".join(map(str, primary_keys))
        hash_object = hashlib.md5(combined_string.encode("utf-8"))
        return hash_object.hexdigest()

"""DynamoDB tap class."""

from __future__ import annotations

import json
import os
from typing import TYPE_CHECKING

from nekt_singer_sdk import Tap
from nekt_singer_sdk import typing as th
from nekt_singer_sdk.streams.core import REPLICATION_FULL_TABLE

from tap_dynamodb import streams
from tap_dynamodb.connectors.aws_boto_connector import AWS_AUTH_CONFIG
from tap_dynamodb.dynamodb_connector import DynamoDbConnector

if TYPE_CHECKING:
    from nekt_singer_sdk.plugin_base import PluginBase


class TapDynamoDB(Tap):
    """DynamoDB tap class."""

    name = "tap-dynamodb"
    package_name = "meltanolabs-tap-dynamodb"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "tables",
            th.ArrayType(th.StringType),
            description="An array of table names to extract from.",
        ),
        th.Property(
            "infer_schema_sample_size",
            th.IntegerType,
            description="The amount of records to sample when inferring the schema.",
            default=100,
        ),
        th.Property(
            "table_scan_kwargs",
            th.ObjectType(),
            description=(
                "A mapping of table name to the scan kwargs that should be used to "
                "override the default when querying that table."
            ),
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.TableStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        dynamodb_conn = DynamoDbConnector(
            dict(self.config),  # type: ignore
        )
        discovered_streams = []
        for table_name in self.config.get("tables") or dynamodb_conn.list_tables():
            tap_metadata = json.loads(os.environ.get(f"{self._env_var_prefix}_METADATA", "{}"))
            stream_metadata = tap_metadata.get(table_name, {})
            replication_key: str | None = stream_metadata.get("replication-key") or "DT"
            replication_method: str = stream_metadata.get("replication-method", REPLICATION_FULL_TABLE)
            stream = streams.TableStream(
                tap=self,
                name=table_name,
                dynamodb_conn=dynamodb_conn,
                infer_schema_sample_size=self.config.get("infer_schema_sample_size"),
                replication_key=replication_key,
                replication_method=replication_method,
            )
            discovered_streams.append(stream)

        return discovered_streams

    @classmethod
    def append_builtin_config(cls: type[PluginBase], config_jsonschema: dict) -> None:
        """Append the built-in config JSON schema for this tap."""

        def _merge_missing(source_jsonschema: dict, target_jsonschema: dict) -> None:
            # Append any missing properties in the target with those from source.
            for k, v in source_jsonschema["properties"].items():
                if k not in target_jsonschema["properties"]:
                    target_jsonschema["properties"][k] = v

        _merge_missing(AWS_AUTH_CONFIG, config_jsonschema)
        super().append_builtin_config(config_jsonschema)  # type: ignore


if __name__ == "__main__":
    TapDynamoDB.cli()

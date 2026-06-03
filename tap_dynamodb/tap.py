"""DynamoDB tap class."""

from __future__ import annotations

import json
import os
from typing import TYPE_CHECKING, Any

from nekt_singer_sdk import Tap
from nekt_singer_sdk import typing as th
from nekt_singer_sdk.custom_logger import user_logger
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
        th.Property(
            "extraction_mode",
            th.StringType,
            description="The extraction mode to use.",
            default="infer_schema",
            allowed_values=["envelope", "infer_schema"],
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
            tap_metadata = self._extract_metadata_from_env()
            stream_metadata = tap_metadata.get(self._normalize_stream_name(table_name), {})
            replication_key: str | None = stream_metadata.get("replication-key")
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

    def _extract_metadata_from_env(self) -> dict[str, dict[str, Any]]:
        """Extract metadata from Meltano 4+ environment variables.

        Meltano 4+ uses individual env vars like:
        TAP_DYNAMODB__METADATA_SAMPLE_MFLIX_MOVIES_REPLICATION_METHOD=INCREMENTAL
        TAP_DYNAMODB__METADATA_SAMPLE_MFLIX_MOVIES_REPLICATION_KEY=lastupdated

        Returns:
            Dictionary mapping stream names to their metadata settings.
        """
        tap_metadata: dict[str, dict[str, Any]] = {}
        metadata_prefix = f"{self._env_var_prefix}_METADATA_"

        # Known metadata setting suffixes (in uppercase with underscores)
        known_settings = [
            "REPLICATION_METHOD",
            "REPLICATION_KEY",
            "SELECTED",
            "SELECTED_BY_DEFAULT",
        ]

        for env_key, env_value in os.environ.items():
            if env_key.startswith(metadata_prefix):
                # Extract the remainder after the metadata prefix
                # Format: {STREAM_NAME}_{SETTING_NAME}
                remainder = env_key[len(metadata_prefix) :]

                # Try to match known setting suffixes
                stream_name = None
                setting_name = None

                for known_setting in known_settings:
                    if remainder.endswith(f"_{known_setting}"):
                        # Extract stream name by removing the setting suffix
                        stream_name_upper = remainder[: -len(known_setting) - 1]
                        stream_name = self._normalize_stream_name(stream_name_upper)
                        setting_name = known_setting
                        break

                if not stream_name or not setting_name:
                    self.logger.debug(f"Skipping unknown metadata env var: {env_key}")
                    continue

                # Convert setting name to lowercase with hyphens (Singer spec format)
                # REPLICATION_METHOD -> replication-method
                setting_key = setting_name.lower().replace("_", "-")

                # Initialize stream metadata if not exists
                if stream_name not in tap_metadata:
                    tap_metadata[stream_name] = {}

                tap_metadata[stream_name][setting_key] = env_value

        if tap_metadata:
            self.logger.info(f"Extracted metadata from env vars: {tap_metadata}")
        else:
            self.logger.info("No metadata found in environment variables")

        return tap_metadata

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

    @staticmethod
    def _normalize_stream_name(name: str) -> str:
        """Normalize a stream name for metadata lookup.

        Meltano converts stream names to uppercase env var segments,
        replacing hyphens with underscores. We normalize to lowercase
        with underscores so both sides match.
        """
        return name.lower().replace("-", "_")


if __name__ == "__main__":
    TapDynamoDB.cli()

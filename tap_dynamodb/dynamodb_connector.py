"""DynamoDB connector class."""

import sys

import genson
import orjson
from botocore.exceptions import ClientError
from mypy_boto3_dynamodb import DynamoDBClient, DynamoDBServiceResource
from nekt_singer_sdk import typing as th
from nekt_singer_sdk.custom_logger import user_logger

from tap_dynamodb.connectors.aws_boto_connector import AWSBotoConnector
from tap_dynamodb.schema_helper import (
    make_properties_nullable,
    recursively_drop_required,
)


class DynamoDbConnector(AWSBotoConnector[DynamoDBServiceResource, DynamoDBClient]):
    """DynamoDB connector class."""

    def __init__(
        self,
        config: dict,
    ) -> None:
        """Initialize the connector.

        Args:
            config: The connector configuration.
        """
        super().__init__(config, "dynamodb")

    @staticmethod
    def _coerce_types(record):
        return orjson.loads(
            orjson.dumps(
                record,
                default=lambda o: str(o),
                option=orjson.OPT_OMIT_MICROSECONDS,
            ).decode("utf-8")
        )

    def list_tables(self, include=None):
        """List tables in DynamoDB."""
        try:
            tables = []
            for table in self.resource.tables.all():
                if include is None or table.name in include:
                    tables.append(table.name)
        except ClientError as err:
            user_logger.error(
                f"Couldn't list tables. Here's why: {err.response['Error']['Code']}: {err.response['Error']['Message']}"
            )
            sys.exit(1)
        else:
            return tables

    def get_items_iter(self, table_name: str, scan_kwargs_override: dict):
        """Get items from a table in DynamoDB."""
        scan_kwargs = scan_kwargs_override.copy()
        if "ConsistentRead" not in scan_kwargs:
            scan_kwargs["ConsistentRead"] = True

        table = self.resource.Table(table_name)
        try:
            done = False
            start_key = None
            while not done:
                if start_key:
                    scan_kwargs["ExclusiveStartKey"] = start_key
                response = table.scan(**scan_kwargs)
                yield [self._coerce_types(record) for record in response.get("Items", [])]
                start_key = response.get("LastEvaluatedKey", None)
                done = start_key is None
        except ClientError as err:
            user_logger.error(
                f"Couldn't scan for {table_name}. Here's why: {err.response['Error']['Code']}: {err.response['Error']['Message']}"
            )
            sys.exit(1)

    def _get_sample_records(self, table_name: str, sample_size: int, scan_kwargs_override: dict) -> list:
        scan_kwargs = scan_kwargs_override.copy()
        sample_records = []
        if "ConsistentRead" not in scan_kwargs:
            scan_kwargs["ConsistentRead"] = True
        if "Limit" not in scan_kwargs:
            scan_kwargs["Limit"] = sample_size

        for batch in self.get_items_iter(table_name, scan_kwargs):
            sample_records.extend(batch)
            if len(sample_records) >= sample_size:
                break
        return sample_records

    def get_table_json_schema(self, table_name: str, sample_size, scan_kwargs: dict, strategy: str = "infer") -> dict:
        """Get the JSON schema for a table in DynamoDB."""
        sample_records = self._get_sample_records(table_name, sample_size, scan_kwargs)

        if not sample_records:
            user_logger.warning(f"No records found for table '{table_name}', generating empty schema.")
            self._primary_keys = self.get_table_key_properties(table_name)
            properties = [th.Property(key, th.StringType) for key in self._primary_keys]
            return th.PropertiesList(*properties).to_dict()
        if strategy == "infer":
            builder = genson.SchemaBuilder(schema_uri=None)
            for record in sample_records:
                builder.add_object(self._coerce_types(record))
            schema = builder.to_schema()
            recursively_drop_required(schema)
            make_properties_nullable(schema)
            if not schema:
                user_logger.error("Inferring schema failed.")
                sys.exit(1)
            else:
                user_logger.info(f"Inferring schema successful for table: '{table_name}'.")
        else:
            user_logger.error(f"Strategy {strategy} not supported.")
            sys.exit(1)
        return schema

    def get_table_key_properties(self, table_name):
        """Get the key properties for a table in DynamoDB."""
        key_schema = self.resource.Table(table_name).key_schema
        return [key.get("AttributeName") for key in key_schema]
        """Get the key properties for a table in DynamoDB."""
        key_schema = self.resource.Table(table_name).key_schema
        return [key.get("AttributeName") for key in key_schema]

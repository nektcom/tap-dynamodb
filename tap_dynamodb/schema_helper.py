def recursively_drop_required(schema: dict) -> None:
    """Recursively drop the required property from a schema.

    This is used to clean up genson generated schemas which are strict by default.

    Args:
        schema: The json schema.
    """
    schema.pop("required", None)
    if "properties" in schema:
        for prop in schema["properties"]:
            if schema["properties"][prop].get("type") == "object":
                recursively_drop_required(schema["properties"][prop])


def make_properties_nullable(schema):
    if isinstance(schema, dict):
        # If this is a dictionary, iterate over its items
        for key, value in schema.items():
            # If the key is "type"
            if key == "type":
                if isinstance(value, list):
                    # If "type" is a list, just append "null"
                    if "null" not in value:
                        value.append("null")
                else:
                    # Skip it if type is actually a property in the schema
                    if isinstance(value, dict):
                        if "type" in value:
                            value["type"] = [value["type"], "null"]
                            continue

                    # If "type" is a single string, convert it to a list
                    schema["type"] = [value, "null"]
            else:
                # Recursively process the value
                make_properties_nullable(value)
    elif isinstance(schema, list):
        # If this is a list, recursively process each item
        for item in schema:
            make_properties_nullable(item)


def remove_null_only_properties(schema_node):
    """Recursively remove properties that only have 'null' in their type list."""
    if not isinstance(schema_node, dict):
        return

    if "properties" in schema_node and isinstance(schema_node["properties"], dict):
        properties_to_delete = []
        for prop_name, prop_schema in schema_node["properties"].items():
            if isinstance(prop_schema, dict) and "type" in prop_schema:
                prop_types = prop_schema.get("type")
                # Handle cases where type is a list or a single string
                if isinstance(prop_types, list):
                    if all(t == "null" for t in prop_types):
                        properties_to_delete.append(prop_name)
                elif prop_types == "null":
                    properties_to_delete.append(prop_name)

            # Recurse into nested objects
            remove_null_only_properties(prop_schema)

        for prop_name in properties_to_delete:
            del schema_node["properties"][prop_name]

        if "required" in schema_node and isinstance(schema_node["required"], list):
            schema_node["required"] = [prop for prop in schema_node["required"] if prop not in properties_to_delete]

    # Handle array items
    if "items" in schema_node and isinstance(schema_node["items"], dict):
        remove_null_only_properties(schema_node["items"])

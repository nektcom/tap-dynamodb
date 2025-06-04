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

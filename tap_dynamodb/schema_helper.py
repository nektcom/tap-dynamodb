from typing import Any


def cleanup_schema(schema: Any) -> dict:
    def remove_null_items(schema):
        if isinstance(schema, dict):
            # Remove 'required' key if it exists
            schema.pop("required", None)

            # If the schema is a list of nulls, fallback the type to a string
            if schema.get("type") == ["null", "null"]:
                return ["string", "null"]

            # Remove only if { "type": "null" }, but not if it's part of a list
            if schema.get("type") == "null":
                return None  # Remove the entire schema entry if type is exactly null

            # Recursively clean nested schemas
            for key, value in list(schema.items()):
                result = remove_null_items(value)
                if result is None:
                    schema.pop(key, None)  # Remove any fields that result in a 'None' value
                else:
                    schema[key] = result
        elif isinstance(schema, list):
            # Recursively clean for lists of schemas
            schema[:] = [remove_null_items(item) for item in schema if remove_null_items(item) is not None]

        return schema

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

        return schema

    def add_missing_array_items(schema):
        if isinstance(schema, dict):
            if schema.get("type") == "array" and "items" not in schema:
                schema["items"] = {"type": "string"}
            for value in schema.values():
                add_missing_array_items(value)
        elif isinstance(schema, list):
            for item in schema:
                add_missing_array_items(item)
        return schema

    def remove_schema_ambiguity(schema):
        if isinstance(schema, dict):
            if "anyOf" in schema:
                # Check if any of the items is of type "array"
                array_type = next((item for item in schema["anyOf"] if item.get("type") == "array"), None)

                # If we found an array type, replace the anyOf with it
                if array_type:
                    return array_type
                else:
                    # If no array type is present, return the first item in anyOf
                    return remove_schema_ambiguity(schema["anyOf"][0])

            # Recursively process each key-value pair
            return {key: remove_schema_ambiguity(value) for key, value in schema.items()}

        elif isinstance(schema, list):
            # Recursively process each item in the list
            return [remove_schema_ambiguity(item) for item in schema]

        return schema

    # Apply all transformations
    schema = remove_null_items(schema)
    schema = remove_schema_ambiguity(schema)
    schema = add_missing_array_items(schema)
    schema = make_properties_nullable(schema)

    return schema

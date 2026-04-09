"""Helpers for classifying schema drift severity."""


def split_path_and_type(schema_path):
    """Split a schema path like $.field:string into path and type."""
    if ":" not in schema_path:
        return schema_path, "unknown"
    path, value_type = schema_path.rsplit(":", 1)
    return path, value_type


def find_type_changes(added_paths, removed_paths):
    """Find paths where the path stayed the same but the value type changed."""
    added_by_path = {}
    removed_by_path = {}

    for schema_path in added_paths:
        path, value_type = split_path_and_type(schema_path)
        added_by_path.setdefault(path, set()).add(value_type)

    for schema_path in removed_paths:
        path, value_type = split_path_and_type(schema_path)
        removed_by_path.setdefault(path, set()).add(value_type)

    type_changes = []
    for path in sorted(set(added_by_path) & set(removed_by_path)):
        type_changes.append(
            {
                "path": path,
                "old_types": sorted(removed_by_path[path]),
                "new_types": sorted(added_by_path[path]),
            }
        )

    return type_changes


def classify_drift(added_paths, removed_paths):
    """Classify drift into green, yellow, or red."""
    type_changes = find_type_changes(added_paths, removed_paths)

    if not added_paths and not removed_paths:
        return {
            "status": "GREEN",
            "summary": "No schema change detected.",
            "type_changes": [],
        }

    if removed_paths or type_changes:
        return {
            "status": "RED",
            "summary": "Schema changed in a breaking way.",
            "type_changes": type_changes,
        }

    return {
        "status": "YELLOW",
        "summary": "Schema changed by adding new fields only.",
        "type_changes": [],
    }
import re


def _camel_case_to_snake_case(name: str) -> str:
    """
    Convert a camelCase identifier into snake_case.
    """
    first_pass = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    second_pass = re.sub("([a-z])([A-Z])", r"\1_\2", first_pass)
    return second_pass.lower()


def rename_column_camel_case_to_snake_case(columns: list[str]) -> list[str]:
    """
    Rename a list of column names from camelCase into snake_case.
    """
    return [_camel_case_to_snake_case(col) for col in columns]

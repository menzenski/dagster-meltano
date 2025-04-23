from typing import Dict, Any

class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


def generate_dagster_name(input_string: str) -> str:
    """Generate a dagster-safe name (^[A-Za-z0-9_]+$.).
    
    Args:
        input_string (str): The input string to generate a dagster-safe name for.

    Returns:
        str: The dagster-safe name.
    """
    return input_string.replace("-", "_").replace(" ", "_").replace(":", "_").replace("=", "_")


def generate_dbt_group_name(node_info: Dict[str, Any]) -> str:
    """Generate the name of the Dagster asset group a DBT nodes lives in.

    This will be namespaced with an extra group if the DBT models are nested in
    sub-folders.

    Args:
        node_info (dict): The information generated for this node by DBT.

    Returns:
        str: The name of the asset group.
    """
    if len(node_info.get("fqn", [])) >= 3:
        return "_".join(node_info["fqn"][1:-1])

    return "dbt"

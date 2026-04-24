"""
fts_framework.auth.env_loader
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Minimal KEY=VALUE .env file parser.

os.environ always wins over .env file values.  Lines beginning with ``#``
and blank lines are silently skipped.  Inline comments are not supported.
Quoted values have the outer quote pair stripped if present.
"""

import os


def load_env_file(path):
    # type: (str) -> dict
    """Parse *path* as a KEY=VALUE file and return a dict of resolved values.

    Resolution: os.environ wins over file values.  The returned dict always
    reflects the effective value for each key in the file; keys already set
    in the environment are included with their environment value.

    Args:
        path (str): Path to the .env file.

    Returns:
        dict: Mapping of variable name to resolved string value.

    Raises:
        IOError: If *path* cannot be opened.
    """
    file_values = {}
    with open(path, "r") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, _, val = line.partition("=")
            key = key.strip()
            val = val.strip()
            if (val.startswith('"') and val.endswith('"')) or \
               (val.startswith("'") and val.endswith("'")):
                val = val[1:-1]
            file_values[key] = val

    result = {}
    for key, file_val in file_values.items():
        result[key] = os.environ.get(key, file_val)
    return result


def resolve_var(var_name, env_vars, env_file_vars):
    # type: (str, dict, dict) -> str
    """Return the value of *var_name*, with os.environ winning over .env file.

    Args:
        var_name (str): Environment variable name.
        env_vars (dict): os.environ (or subset).
        env_file_vars (dict): Parsed .env file values.

    Returns:
        str or None: The resolved value, or None if not set in either source.
    """
    return env_vars.get(var_name) or env_file_vars.get(var_name) or None

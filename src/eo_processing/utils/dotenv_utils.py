import os
from dotenv import load_dotenv, find_dotenv, set_key

DOTENV = find_dotenv()

if os.path.exists(DOTENV) and DOTENV:
    load_dotenv(DOTENV)


def set_dotenv_vars_from_dict(
        var_dict: dict):
    """
    Adds or Updates a key/value from a dictionary to the given .env environment variables.

    :param credentials (dict): Mapping of env var names to values.
    """
    for key, value in var_dict.items():
        if value is None:
            continue
        os.environ[key] = value

        if os.path.exists(DOTENV):
            set_key(DOTENV, key, value)

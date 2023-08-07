from pathlib import Path


def infuse(string: str, params: dict) -> str:
    """Replace all ${key} in string with value from params dict.
    Preserves SQL literal based on type of value:
     - str  -> 'val'
     - list -> 'val1','val2','val3'
     - else -> val
    Example:
        >>> query = infuse(
        >>> 'SELECT * '
        >>> 'FROM ${database}.${table} '
        >>> 'WHERE username = ${username} AND user_type IN (${user_types}) '
        >>> 'LIMIT ${limit}',
        >>> params={
        >>>    'database': Expr('test'),
        >>>    'table': Expr('test_table'),
        >>>    'username': 'test_user',
        >>>    'user_types': ['user', 'admin'],
        >>>    'limit': 10
        >>>    }
        >>> )
        >>> print(query)
        Output: SELECT * FROM test.test_table WHERE username = 'test_user' AND user_type IN ('user','admin') LIMIT 10
    """
    if not params:
        return string

    result = string
    for key, value in params.items():
        replace_value = value

        if type(value) is list:
            replace_value = ','.join([f"'{val}'" for val in value])
        elif type(value) is str:
            replace_value = f"'{value}'"

        result = result.replace(
            '${%s}' % key,
            str(replace_value)
        )

    return result


def load_query(path: Path, params: dict = None) -> str:
    """Load query from resources folder and replace params in it."""
    with path.open() as query:
        return infuse(query.read(), params)


class Expr:
    """Wrap string with this class to prevent infuse from adding quotes around the value."""

    def __init__(self, content: str):
        self.content = content

    def __eq__(self, other): return self.content == other.content

    def __str__(self): return self.content

    def __repr__(self): return self.content

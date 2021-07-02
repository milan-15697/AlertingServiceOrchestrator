#!/usr/bin/python
import os
def config():
    """Credentials of postgres Database"""
    postgres_db = {}
    params = [('host', os.environ['HOTPATH_DB_HOST']),
              ('database', 'eas'),
              ('user', 'postgres'),
              ('password', 'ster123')]
    for param in params:
        postgres_db[param[0]] = param[1]
    return postgres_db


import os
from functools import wraps

from flask import request


def check_auth(username, password):
    return username == 'hackclub' and password == os.environ["SINERIDER_TWITTER_API_KEY"]

def login_required(f):
    """ basic auth for api """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return "Authentication required", 401
        return f(*args, **kwargs)
    return decorated_function

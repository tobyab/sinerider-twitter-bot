import os
from functools import wraps

from flask import request


def check_auth(username, password):
    """ Validate submitted passwords
    :param username: The username
    :param password: The password
    :return: True if authenticated, otherwise False
    """
    return username == 'hackclub' and password == os.environ["SINERIDER_TWITTER_API_KEY"]

def login_required(f):
    """ Decorator that allows to flag Flask endpoints as requiring basic authentication. """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return "Authentication required", 401
        return f(*args, **kwargs)
    return decorated_function

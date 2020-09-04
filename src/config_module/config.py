import os
from configparser import ConfigParser


def resolve():
    config_object = ConfigParser()
    config_object.read(os.path.abspath("../config.ini"))
    return config_object

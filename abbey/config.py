import yaml
import os

config_format = {
    "user": str,
    "path": str,
    "db_echo": bool,
}



class Config(object):
    def __init__(self, filename):
        self.config_file = filename
        with open(filename, 'r') as ymlfile:
            cfg = yaml.load(ymlfile)
        for entry,value in cfg.items():
            try:
                assert type(value) == config_format[entry]
            except KeyError:
                raise ValueError("Unknown configuration entry '{}'.".format(entry))
            except AssertionError:
                raise ValueError("Configuration entry '{}' has wrong type: {} instead of {}.".format(
                    entry, type(value), config_format[entry]))
            setattr(self, entry, value)

import yaml
import os

config_format = {
    "user": str,
    "path": str,
    "db_echo": bool,
    "server": str,
}



class Config(object):
    def __init__(self, cfg):
        if isinstance(cfg, str):
            self.config_file = None
            self.read_yaml(cfg)
        else:
            self.read_dict(cfg)

    def read_dict(self, cfg):
        for entry,value in cfg.items():
            try:
                assert type(value) == config_format[entry]
            except KeyError:
                raise ValueError("Unknown configuration entry '{}'.".format(entry))
            except AssertionError:
                raise ValueError("Configuration entry '{}' has wrong type: {} instead of {}.".format(
                    entry, type(value), config_format[entry]))
            setattr(self, entry, value)
        for entry in config_format:
            if not hasattr(self, entry):
                raise ValueError("Could not find setting {} in configuration".format(entry))


    def read_yaml(self, filename):
        self.config_file = filename
        with open(filename, 'r') as ymlfile:
            cfg = yaml.load(ymlfile)
            self.read_dict(cfg)






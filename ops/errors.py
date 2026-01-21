class OpsError(Exception):
    exit_code = 50


class ConfigError(OpsError):
    exit_code = 10


class AdapterError(OpsError):
    exit_code = 20


class DatabaseError(OpsError):
    exit_code = 30


class IOError(OpsError):
    exit_code = 40

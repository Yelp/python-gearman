class GearmanError(Exception):
    pass

class ConnectionError(GearmanError):
    pass

class ServerUnavailable(GearmanError):
    pass

class CommandError(GearmanError):
    pass

class InvalidResponse(GearmanError):
    pass

class ProtocolError(GearmanError):
    pass

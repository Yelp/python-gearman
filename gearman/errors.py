class GearmanError(Exception):
    pass

class ConnectionError(GearmanError):
    pass

class ServerUnavailable(GearmanError):
    pass

class ProtocolError(GearmanError):
    pass

class UnknownCommandError(GearmanError):
    pass

class ExceededConnectionAttempts(GearmanError):
    pass

class InvalidClientState(GearmanError):
    pass

class InvalidWorkerState(GearmanError):
    pass

class InvalidAdminClientState(GearmanError):
    pass

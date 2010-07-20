_DEBUG_MODE_ = False
DEFAULT_GEARMAN_PORT = 4730

PRIORITY_NONE = None
PRIORITY_LOW  = 'LOW'
PRIORITY_HIGH = 'HIGH'

JOB_UNKNOWN  = 'UNKNOWN'  # Request state is currently unknown, either unsubmitted or connection failed
JOB_PENDING  = 'PENDING'  # Request has been submitted, pending handle
JOB_CREATED  = 'CREATED'  # Request has been accepted
JOB_FAILED   = 'FAILED'   # Request received an explicit fail
JOB_COMPLETE = 'COMPLETE' # Request received an explicit complete

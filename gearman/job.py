
class GearmanJob(object):
    def __init__(self, handle, function_name, unique, data, gearman_client=None, gearman_worker=None):
        assert handle, "Missing valid handle"
        assert bool(gearman_client) ^ bool(gearman_worker), "Must specify a gearman_client OR a gearman_worker"
        self.gearman_client = gearman_client
        self.gearman_worker = gearman_worker

        self.handle = handle
        self.func = function_name
        self.unique = unique
        self.data = data

    def __repr__(self):
        return "<GearmanJob handle=%s, func=%s, unique=%s, data=%s>" % (self.conn, self.handle, self.func, self.unique, self.data)

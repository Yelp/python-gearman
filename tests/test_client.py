from gearman import GearmanClient

class Client(GearmanClient):
    id = None
    
    def __init__(self, id, host, port):
        self.id = id
        GearmanClient.__init__(self, (['%s:%d' % (host, port)]))

    def client_submit_job(self, *args):
        return super(Client, self).submit_job(*args)

    def client_submit_multiple_jobs(self, *args):
        print('worker %d: submit_multiple_jobs' % self.id)
        return super(Client, self).submit_multiple_jobs(*args)

    def client_get_job_status(self, *args):
        print('worker %d: get_job_status' % self.id)
        return super(Client, self).get_job_status(*args)


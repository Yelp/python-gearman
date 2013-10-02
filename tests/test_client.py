from gearman import GearmanClient

class Client(GearmanClient):
    id = None
    
    def __init__(self, id, servers):
        self.id = id
        GearmanClient.__init__(self, servers)

    def client_submit_job(self, *args):
        response = super(Client, self).submit_job(*args)
        p =  response.job.connection.gearman_port
        #print('client %d, server %d: submit_job' % (self.id, p))
        return response

    def client_submit_multiple_jobs(self, *args):
        #print('client %d: submit_multiple_jobs' % self.id)
        response_list = super(Client, self).submit_multiple_jobs(*args)
        for response in response_list:
            p = response.job.connection.gearman_port
            #print('\tworker %d, server %d: submit_multiple_jobs' % (self.id, p))
        return response_list

    def client_get_job_status(self, *args):
        #print('worker %d: get_job_status' % self.id)
        response = super(Client, self).get_job_status(*args)
        p =  response.job.connection.gearman_port
        #print('client %d, server %d: get_job_status' % (self.id, p))
        return response


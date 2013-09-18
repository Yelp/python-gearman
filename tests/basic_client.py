import gearman

gm_client = gearman.GearmanClient(['localhost:4730'])

print 'Sending job...'
request = gm_client.submit_job('reverse', 'Hello Py2!')

print "Result: " + str(request.result)

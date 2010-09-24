:mod:`gearman.admin_client` --- Gearman Admin client
====================================================
.. module:: gearman.admin_client
   :synopsis: Gearman admin client - public interface for querying about server status

.. autoclass:: GearmanAdminClient

Interacting with a server
-------------------------
.. automethod:: GearmanAdminClient.send_maxqueue

.. automethod:: GearmanAdminClient.send_shutdown

.. automethod:: GearmanAdminClient.get_status

.. automethod:: GearmanAdminClient.get_version

.. automethod:: GearmanAdminClient.get_workers

Checking server state::

    gm_admin_client = gearman.GearmanAdminClient(['localhost:4730'])

    # Inspect server state
    status_response = gm_admin_client.get_status()
    version_response = gm_admin_client.get_version()
    workers_response = gm_admin_client.get_workers()

Testing server response times
-----------------------------

.. automethod:: GearmanAdminClient.ping_server

Checking server response time::

    gm_admin_client = gearman.GearmanAdminClient(['localhost:4730'])
    response_time = gm_admin_client.ping_server()

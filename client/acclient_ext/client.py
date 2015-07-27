import acclient
from .legacyclient import LegacyClient


class ACFactory(object):
    def get(self, address='localhost', port=9999, password=None, db=0):
        return acclient.Client(address, port, password, db)

    def getRunArgs(self, domain=None, name=None, max_time=0, max_restart=0, recurring_period=0, stats_interval=0,
                   args=None, loglevels='*', loglevels_db=None, loglevels_ac=None):
        """
        Creates a reusable run arguments object

        :domain: Domain name
        :name: script or executable name
        :max_time: Max run time, 0 (forever), -1 forever but remember during reboots (long running),
            other values is timeout
        :max_restart: Max number of restarts if process died in under 5 min.
        :recurring_period: Scheduling time
        :stats_interval: How frequent the stats aggregation is done/flushed to AC
        :args: Command line arguments (in case of execute)
        :loglevels: Which log levels to capture and pass to logger
        :loglevels_db: Which log levels to store in DB (overrides logger defaults)
        :loglevels_ac: Which log levels to send to AC (overrides logger defaults)
        """
        return acclient.RunArgs(domain=domain, name=name, max_time=max_time, max_restart=max_restart,
                                recurring_period=recurring_period, stats_interval=stats_interval, args=args,
                                loglevels=loglevels, loglevels_db=loglevels_db, loglevels_ac=loglevels_ac)

    def getLegacyClient(self, address='localhost', port=9999, password=None, db=0):
        return LegacyClient(address, port, password, db)

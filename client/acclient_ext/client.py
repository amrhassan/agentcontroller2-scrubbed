import acclient


class ACFactory(object):
    def Client(self, address='localhost', port=9999, db=0):
        return acclient.Client(address, port, db)

    def RunArgs(self, domain=None, name=None, max_time=0, max_restart=0, recurring_period=0, stats_interval=0,
                args=None, loglevels=None, loglevels_db=None, loglevels_ac=None):
        return acclient.RunArgs(domain=domain, name=name, max_time=max_time, max_restart=max_restart,
                                recurring_period=recurring_period, stats_interval=stats_interval, args=args,
                                loglevels=loglevels, loglevels_db=loglevels_db, loglevels_ac=loglevels_ac)

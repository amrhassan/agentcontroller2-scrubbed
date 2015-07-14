
from JumpScale import j


j.base.loader.makeAvailable(j, 'clients')


def ac():
    from .client import ACFactory
    return ACFactory()

j.clients._register('ac', ac)

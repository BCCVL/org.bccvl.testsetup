import logging
from org.bccvl.testsetup.main import main

LOG = logging.getLogger(__name__)


def setupVarious(context):
    LOG.info('BCCVL Test content setup handler')
    # only run for this product
    if context.readDataFile('org.bccvl.testsetup.marker.txt') is None:
        return

    # get Zope root
    app = context.getSite().restrictedTraverse('/')
    main(app)

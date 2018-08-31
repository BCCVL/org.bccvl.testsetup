""" call this with:
    ./bin/instance datasetcleanup

    make sure ./bin/instance is down while doing this
"""
import sys
import logging
from AccessControl.SecurityManagement import newSecurityManager
from AccessControl.SpecialUsers import system
from Testing.makerequest import makerequest
import transaction
from AccessControl.SecurityManager import setSecurityPolicy
from Products.CMFCore.tests.base.security import PermissiveSecurityPolicy, OmnipotentUser
from collective.transmogrifier.transmogrifier import Transmogrifier
import argparse
from org.bccvl.site.content.interfaces import IDataset
from org.bccvl.site import defaults
from Products.CMFCore.utils import getToolByName


# TODO: if item/file id already exists, then just updload/update metadata
try:
    from zope.component.hooks import site
except ImportError:
    # we have an older zope.compenents:
    import contextlib
    from zope.component.hooks import getSite, setSite

    @contextlib.contextmanager
    def site(site):
        old_site = getSite()
        setSite(site)
        try:
            yield
        finally:
            setSite(old_site)

LOG = logging.getLogger('org.bccvl.testsetup')


def cleanup_dataset(site, params):
    # Delete datasets that are marked as REMOVED and are not referenced by experiment
    pc = getToolByName(site, 'portal_catalog')

    trcounter = 0
    datasets = site[defaults.DATASETS_FOLDER_ID]
    for brain in pc.unrestrictedSearchResults(object_provides=IDataset.__identifier__,
                                              path='/'.join(datasets.getPhysicalPath()),
                                              job_state='REMOVED'):
        # Fix me: Do a commit on every single delete as some dataset has invalid url.
        # Shall fix the dataset URL, then can commit a series of deletes.
        ds = brain.getObject()
        logger.info("Deleting datasets %d", brain.Title)
        ds.aq_parent.manage_delObjects([ds.getId()])
        transaction.commit()
    return

def spoofRequest(app):
    """
    Make REQUEST variable to be available on the Zope application server.

    This allows acquisition to work properly
    """
    _policy = PermissiveSecurityPolicy()
    _oldpolicy = setSecurityPolicy(_policy)
    newSecurityManager(None, OmnipotentUser().__of__(app.acl_users))
    return makerequest(app)

def main(app, params):
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)

    logging.getLogger('ZODB.Connection').setLevel(logging.WARN)

    app = spoofRequest(app)
    newSecurityManager(None, system)
    # TODO: works only if site id is bccvl
    portal = app.unrestrictedTraverse('bccvl')
    # we didn't traverse, so we have to set the proper site

    with site(portal):
        cleanup_dataset(portal, params)


def parse_args(args):
    parser = argparse.ArgumentParser(description='Cleanup datasets.')
    pargs = parser.parse_args(args)
    return vars(pargs)

def zopectl(app, args):
    """ zopectl entry point
    app ... the Zope root Application
    args ... list of command line args passed (very similar to sys.argv)
             args[0] ... name of script but is always '-c'
             args[1] ... name of entry point
             args[2:] ... all additional commandline args
    """
    # get rid of '-c'
    if args[0] == '-c':
        args.pop(0)
    # now args looks pretty much like sys.argv
    params = parse_args(args[1:])
    # ok let's do some import'
    main(app, params)


if 'app' in locals():
    # we have been started via ./bin/instance run main.py
    # but ideally should be run via ./bin/instance datasetcleanup
    main(app, parse_args(sys.argv[4:]))
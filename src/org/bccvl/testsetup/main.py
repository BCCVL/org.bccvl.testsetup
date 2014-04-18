""" call this with:
    ./bin/instance run src/org.bccvl.testestup/src/org/bccvl/testsetup/main.py ....

    make sure ./bin/instance is down while doing this
"""
import Globals
import urllib
import os
import os.path
from tempfile import mkdtemp
from zipfile import ZipFile, ZIP_DEFLATED
import subprocess
import sys
import shutil
import glob
import logging
from urllib import urlopen
from pkg_resources import resource_listdir, resource_stream
from org.bccvl.site import defaults as bccvldefaults
from org.bccvl.site.namespace import BCCVOCAB, BCCPROP
from AccessControl.SecurityManagement import newSecurityManager
from AccessControl.SpecialUsers import system
from Testing.makerequest import makerequest
import transaction
from AccessControl.SecurityManager import setSecurityPolicy
from Products.CMFCore.tests.base.security import PermissiveSecurityPolicy, OmnipotentUser
from plone.dexterity.utils import createContentInContainer
from plone.i18n.normalizer.interfaces import IFileNameNormalizer
from zope.component import getUtility
from gu.z3cform.rdf.interfaces import IORDF, IGraph
from rdflib import RDF
from plone.namedfile.file import NamedBlobFile
from collective.transmogrifier.transmogrifier import Transmogrifier
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


def import_data(site, params):
    transmogrifier = Transmogrifier(site)
    options = {}
    for opt in ('gcm', 'emsc', 'year'):
        if opt in params:
            options[opt] = '\n'.join(params[opt])
    transmogrifier(u'org.bccvl.testsetup.dataimport',
                   source={'path': 'org.bccvl.testsetup:data'},
                   downloader=options)
    transaction.commit()


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

    logging.getLogger('gu.plone.rdf.subscriber').setLevel(logging.WARN)
    logging.getLogger('gu.plone.rdf.repositorymetadata').setLevel(logging.WARN)
    logging.getLogger('gu.z3cform.rdf.ordfhandler').setLevel(logging.WARN)
    logging.getLogger('ZODB.Connection').setLevel(logging.WARN)
    logging.getLogger('ordf.handler.httpfourstore').setLevel(logging.WARN)
    # logging.getLogger('').setLevel(logging.WARN)
    # logging.getLogger('').setLevel(logging.WARN)

    app = spoofRequest(app)
    newSecurityManager(None, system)
    # TODO: works only if site id is bccvl
    portal = app.unrestrictedTraverse('bccvl')
    # we didn't traverse, so we have to set the proper site
    with site(portal):
        import_data(portal, params)


def parse_args(args):
    arglist = {'--gcm': 'gcm',
               '--emsc': 'emsc',
               '--year': 'year'}
    result = {}
    for i in xrange(0, len(args), 2):
        try:
            name = args[i].strip()
            i += 1
            value = args[i].strip()
            if name in arglist:
                # TODO: validate value based on possible values for name
                key = arglist[name]
                if key not in result:
                    result[key] = set()
                result[key].add(value)
                # break out here... everything below is error handling
                continue
        except Exception as e:
            print "Error:", e
        # if we reach this then there was an error
        result['help'] = set()
        break
    return result


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
    if len(args) <= 1:
        # we don't have some cli args, let's print usage and exit
        usage()
        exit(1)
    params = parse_args(args[1:])
    if 'help' in params:
        # user requested help
        usage()
        exit(0)
    # ok let's do some import'
    main(app, params)


def usage():
    print " accepted arguments: "
    print "   all arguments can be given multiple times"
    print " --gcm <GCM>"
    print " --emsc <EMSC>"
    print " --year <yyyy>"
    # TODO: print supported list of gcm, emsc, and years as well


if 'app' in locals():
    # we have been started via ./bin/instance run main.py
    main(app)

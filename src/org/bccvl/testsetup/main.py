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


def import_data(site):
    transmogrifier = Transmogrifier(site)
    transmogrifier(u'org.bccvl.testsetup.dataimport',
                   source={'path': 'org.bccvl.testsetup:data'})
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


def main(app):
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
        import_data(portal)




BIOCLIM_DATA = [
    {'title': u'Climate Projection RCP3D based on CCCma-CGCGM3, 0.5arcmin (~1km)',
     'agency': u'CCCma',
     'model': u'CGCM3',
     'url': u'http://wallaceinitiative.org/climate_2012/output/australia-1km/RCP3PD_cccma-cgcm31.zip',
     'genre': BCCVOCAB['DataGenreFC'],
     'type': 'link'
    }

    ]





if 'app' in locals():
    # we have been started via ./bin/instance run main.py
    main(app)

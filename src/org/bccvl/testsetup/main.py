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
from gu.repository.content.interfaces import IRepositoryMetadata
from gu.z3cform.rdf.interfaces import IORDF
from rdflib import RDF


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



def main(app):
    """

    app ... Zope application server root
    sys.args ... any additional cli paramaters given
    """
    #import ipdb; ipdb.set_trace()
    app = spoofRequest(app)
    newSecurityManager(None, system)
    get_current_bioclim_data()
    add_enviro_data(app)
    add_occurence_data(app)

def spoofRequest(app):
    """
    Make REQUEST variable to be available on the Zope application server.

    This allows acquisition to work properly
    """
    _policy = PermissiveSecurityPolicy()
    _oldpolicy = setSecurityPolicy(_policy)
    newSecurityManager(None, OmnipotentUser().__of__(app.acl_users))
    return makerequest(app)

def addFile(content, filename, file=None, mimetype='application/octet-stream'):
    normalizer = getUtility(IFileNameNormalizer)
    linkid = normalizer.normalize(os.path.basename(filename))
    if linkid in content:
        return content[linkid]
    if file is None:
        file = urlopen(filename)
    linkid = content.invokeFactory(type_name='File', id=linkid, title=os.path.basename(filename),
                                   file=file.read())
    linkcontent = content[linkid]
    linkcontent.setFormat(mimetype)
    linkcontent.setFilename(filename.encode('utf-8'))
    linkcontent.processForm()
    return linkcontent


def addItem(folder, title, subject=None, description=None, id=None):
    if id is not None and id in folder:
        return folder[id]
    content = createContentInContainer(folder, 'gu.repository.content.RepositoryItem',
                                       title=title, subjec=subject, id=id)
    if id is not None and id != content.id:
        # need to commit here, otherwise _p_jar is None and rename fails
        transaction.savepoint(optimistic=True)
        LOG.info("Rename %s to %s", content.id, id)
        folder.manage_renameObject(content.id, id)
    return folder[content.id]


def add_enviro_data(app):
    portal = app.unrestrictedTraverse('bccvl')
    # set plone site as current site to enable local utility lookup
    with site(portal):
        portal.setupCurrentSkin(app.REQUEST)
        folder = portal.unrestrictedTraverse('{}/{}'.format(bccvldefaults.DATASETS_FOLDER_ID, bccvldefaults.DATASETS_ENVIRONMENTAL_FOLDER_ID))
        content = addItem(folder,
                          title=u'Current climate layers for Australia, 2.5arcmin (~5km)',
                          id='current')
        cgraph = IRepositoryMetadata(content)
        cgraph.add((cgraph.identifier, BCCPROP['datagenre'], BCCVOCAB['DataGenreE']))
        contentzip = addFile(content,
                             filename=u'file://' + os.path.abspath(os.path.join(Globals.data_dir, 'current_asc.zip')),
                             mimetype='application/zip')

        rdfhandler = getUtility(IORDF).getHandler()
        cc = rdfhandler.context(user='Importer', reason="auto import content")
        # store modified data
        cc.add(cgraph)
        # send changeset
        cc.commit()

        transaction.commit()
    app._p_jar.sync()

    
def add_occurence_data(app):
    portal = app.unrestrictedTraverse('bccvl')
    # set plone site as current site to enable local utility lookup
    with site(portal):
        portal.setupCurrentSkin(app.REQUEST)
        folder = portal.unrestrictedTraverse('{}/{}'.format(bccvldefaults.DATASETS_FOLDER_ID, bccvldefaults.DATASETS_SPECIES_FOLDER_ID))
        for dirname in resource_listdir('org.bccvl.testsetup', 'data/species'):
            data = resource_stream('org.bccvl.testsetup', os.path.join('data/species', dirname, 'occur.csv'))
            content = addItem(folder,
                              title=u'Occurence Data for {}'.format(dirname),
                              id=dirname.encode('utf-8'))
            cgraph = IRepositoryMetadata(content)
            cgraph.add((cgraph.identifier, BCCPROP['datagenre'], BCCVOCAB['DataGenreSO']))
            contentfile = addFile(content,
                                  filename=u'occur.csv',
                                  file=data,
                                  mimetype='text/csv')

            rdfhandler = getUtility(IORDF).getHandler()
            cc = rdfhandler.context(user='Importer', reason="auto import content")
            # store modified data
            cc.add(cgraph)
            # send changeset
            cc.commit()

            transaction.commit()
    app._p_jar.sync()

        


def get_current_bioclim_data():
    final = os.path.join(Globals.data_dir, 'current_asc.zip')
    current = os.path.join(Globals.data_dir, 'current.zip') # aka CLIENT_HOME
    if os.path.exists(final):
        return
    if not os.path.exists(current):
        LOG.info('Download current climate layers to %s', current)
        urllib.urlretrieve('http://wallaceinitiative.org/climate_2012/output/australia-5km/current.zip',
                           current)
    tmp_dir = mkdtemp(dir=Globals.data_dir)
    curzip = ZipFile(current, 'r')
    curzip.extractall(path=tmp_dir)
    curzip.close()
    cmd = ['gunzip']
    cmd.extend(glob.glob(os.path.join(tmp_dir, '*', '*.gz')))
    ret = subprocess.call(cmd)
    if ret != 0:
        LOG.fatal('Uncompressing asc files failed')
        sys.exit(1)
    with ZipFile(final, 'w', ZIP_DEFLATED) as newzip:
        for ascname in sorted(os.listdir(os.path.join(tmp_dir, 'current.76to05'))):
            newzip.write(os.path.join(tmp_dir, 'current.76to05', ascname),
                         os.path.join('current.76to05', ascname))
    shutil.rmtree(tmp_dir)



if 'app' in locals():
    # we have been started via ./bin/instance run main.py
    main(app)

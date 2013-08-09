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


BIOCLIM_DATA = [
    {'title': u'Current climate layers for Australia, 2.5arcmin (~5km)',
     #'model': u'CGCM3',
     'url': u'http://wallaceinitiative.org/climate_2012/output/australia-5km/current.zip',
     'filename': u'current.zip'

    },
    {'title': u'Climate Projection RCP3D based on CCCma-CGCGM3 ',
     'agency': u'CCCma',
     'model': u'CGCM3',
     'url': u'http://wallaceinitiative.org/climate_2012/output/australia-5km/RCP3PD_cccma-cgcm31.zip'
    }
    ]


ALGORITHM_DATA = [
    {'title': u"BIOCLIM",
     'id': 'bioclim'
    },
    {'title': u"Boosted Regression Trees",
     'id': 'brt',
    },
    ]


def main(app):
    """

    app ... Zope application server root
    sys.args ... any additional cli paramaters given
    """
    app = spoofRequest(app)
    newSecurityManager(None, system)
    get_current_bioclim_data(BIOCLIM_DATA)
    add_enviro_data(app, BIOCLIM_DATA)
    add_occurence_data(app)
    add_algorithm(app, ALGORITHM_DATA)


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
    linkid = content.invokeFactory(type_name='File', id=linkid,
                                   title=os.path.basename(filename),
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


def add_algorithm(app, data):
    # TODO: this will probably end up being something different and not a
    #       RepositoryItem
    portal = app.unrestrictedTraverse('bccvl')
    # set plone site as current site to enable local utility lookup
    with site(portal):
        portal.setupCurrentSkin(app.REQUEST)
        folder = portal.unrestrictedTraverse('{}/{}'.format(bccvldefaults.DATASETS_FOLDER_ID,
                                                            bccvldefaults.FUNCTIONS_FOLDER_ID))
        for algo in data:
            content = addItem(folder,
                              title=data['title'],
                              id=data['id'])
        transaction.commit()
    app._p_jar.sync()


def add_enviro_data(app, data):
    portal = app.unrestrictedTraverse('bccvl')
    # set plone site as current site to enable local utility lookup
    with site(portal):
        portal.setupCurrentSkin(app.REQUEST)
        folder = portal.unrestrictedTraverse('{}/{}'.format(bccvldefaults.DATASETS_FOLDER_ID, bccvldefaults.DATASETS_ENVIRONMENTAL_FOLDER_ID))
        for item in data:
            zipfile =  os.path.basename(item['url'])
            content = addItem(folder, title=item['title'],
                              id=os.path.splitext(zipfile)[0])
            cgraph = IRepositoryMetadata(content)
            cgraph.add((cgraph.identifier, BCCPROP['datagenre'],
                        BCCVOCAB['DataGenreE']))
            contentzip = addFile(content,
                                 filename=u'file://' + os.path.join(Globals.data_dir, zipfile),
                                 mimetype='application/zip')
            # TODO: attach proper metadat to files (probably needs inspection of zip to find out layers and filenames)
            rdfhandler = getUtility(IORDF).getHandler()
            cc = rdfhandler.context(user='Importer',
                                    reason="auto import content")
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
            content = addItem(folder,
                              title=u'Occurence Data for {}'.format(dirname),
                              id=dirname.encode('utf-8'))
            for data in resource_listdir('org.bccvl.testsetup', 'data/species/' +  dirname):
                # TODO: files sholud get metadat as well
                content = resource_stream('org.bccvl.testsetup', data)
                contentfile = addFile(content,
                                      filename=unicode(os.path.basename(data)),
                                      file=data,
                                      mimetype='text/csv')

            cgraph = IRepositoryMetadata(content)
            # TODO: fixup data genre
            cgraph.add((cgraph.identifier, BCCPROP['datagenre'],
                        BCCVOCAB['DataGenreSO']))

            rdfhandler = getUtility(IORDF).getHandler()
            cc = rdfhandler.context(user='Importer',
                                    reason="auto import content")
            # store modified data
            cc.add(cgraph)
            # send changeset
            cc.commit()

            transaction.commit()
    app._p_jar.sync()


def get_current_bioclim_data(data):
    for item in data:
        filename = os.path.basename(item['url'])
        filename = os.path.join(Globals.data_dir, filename)
        if os.path.exists(filename):
            continue
        LOG.info("Download %s", item['title'])
        urllib.urlretrieve(item['url'],  filename)
        # the files from wallace_initiative are all gzipped asc files, so let's
        # convert them here
        tmp_dir = mkdtemp(dir=Globals.data_dir)
        with ZipFile(filename, 'r') as curzip:
            curzip.extractall(path=tmp_dir)
        # unzip all .asc.gz files
        cmd = ['gunzip']
        cmd.extend(glob.glob(os.path.join(tmp_dir, '*', '*.gz')))
        ret = subprocess.call(cmd)
        if ret != 0:
            LOG.fatal('Uncompressing asc files for %s failed', filename)
            sys.exit(1)
        # convert all files to geotiff
        for ascfile in glob.glob(os.path.join(tmp_dir, '*', '*.asc')):
            tfile, _ = os.path.splitext(ascfile)
            tfile += '.tif'
            cmd = ['gdal_translate', 'of=GTiff', ascfile,  tfile]
            ret = subprocess.call(cmd)
            if ret != 0:
                LOG.fatal('Conversion to GeoTiff for %s failed', ascfile)
                sys.exit(1)
        # create a new zip file with same name as downloaded one
        with ZipFile(filename, 'w', ZIP_DEFLATED) as newzip:
            for filename in sorted(glob.glob(os.path.join(tmp_dir,  '*',  '*.tif'))):
                _, dirname = os.path.split(os.path.dirname(filename))
                newzip.write(filename,
                             os.path.join(dirname, os.path.basename(filename)))
    shutil.rmtree(tmp_dir)



if 'app' in locals():
    # we have been started via ./bin/instance run main.py
    main(app)

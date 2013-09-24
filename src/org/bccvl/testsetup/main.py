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
     'filename': u'current.zip',
     'genre': BCCVOCAB['DataGenreE'],
     'type': 'file'
    },
    {'title': u'Climate Projection RCP3D based on CCCma-CGCGM3, 2.5arcmin (~5km)',
     'agency': u'CCCma',
     'model': u'CGCM3',
     'url': u'http://wallaceinitiative.org/climate_2012/output/australia-5km/RCP3PD_cccma-cgcm31.zip',
     'genre': BCCVOCAB['DataGenreFC'],
     'type': 'file'
    },
    {'title': u'Climate Projection RCP3D based on CCCma-CGCGM3, 0.5arcmin (~1km)',
     'agency': u'CCCma',
     'model': u'CGCM3',
     'url': u'http://wallaceinitiative.org/climate_2012/output/australia-1km/RCP3PD_cccma-cgcm31.zip',
     'genre': BCCVOCAB['DataGenreFC'],
     'type': 'link'
    }

    ]


ALGORITHM_DATA = [
    {'title': u"Bioclim",
     'id': 'bioclim',
     'method': 'org.bccvl.compute.bioclim.execute'
    },
    {'title': u"Boosted Regression Trees",
     'id': 'brt',
     'method': 'org.bccvl.compute.brt.execute'
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


def addDataset(content, filename, file=None, mimetype='application/octet-stream'):
    normalizer = getUtility(IFileNameNormalizer)
    linkid = normalizer.normalize(os.path.basename(filename))
    if linkid in content:
        return content[linkid]
    if file is None:
        # can't handle url's in _setData of blob has to be a local file
        # TODO: maybe create IStorage adapter for urllib.urlinfo class and use urlopen again
        file = open(filename)
    linkid = content.invokeFactory(type_name='org.bccvl.content.dataset', id=linkid,
                                   title=content.title)

    linkcontent = content[linkid]
    linkcontent.setFormat(mimetype)
    linkcontent.file = NamedBlobFile(contentType=mimetype, filename=unicode(linkid))
    linkcontent.file.data = file
    return linkcontent


def addLink(content, url):
    normalizer = getUtility(IFileNameNormalizer)
    linkid, _ = os.path.splitext(os.path.basename(url))
    linkid = normalizer.normalize(linkid)
    if linkid in content:
        return content[linkid]
    linkid = content.invokeFactory(type_name='Link', id=linkid,
                                   title=os.path.basename(url),
                                   url=url)
    return content[linkid]


def addItem(folder, **kw):
    if 'portal_type' not in kw:
        kw['portal_type'] = 'gu.repository.content.RepositoryItem'
    id = kw.get('id', None)
    if id is not None and id in folder:
        return folder[id]
    content = createContentInContainer(folder, **kw)
    if id is not None and id != content.id:
        # need to commit here, otherwise _p_jar is None and rename fails
        transaction.savepoint(optimistic=True)
        LOG.info("Rename %s to %s", content.id, str(id))
        folder.manage_renameObject(content.id, str(id))
    return folder[content.id]


def add_algorithm(app, data):
    # TODO: this will probably end up being something different and not a
    #       RepositoryItem
    portal = app.unrestrictedTraverse('bccvl')
    # set plone site as current site to enable local utility lookup
    with site(portal):
        portal.setupCurrentSkin(app.REQUEST)
        # TODO: are we acquiring functions folder here? it should be child of portal not datasets
        folder = portal.unrestrictedTraverse('{}/{}'.format(bccvldefaults.DATASETS_FOLDER_ID,
                                                            bccvldefaults.FUNCTIONS_FOLDER_ID))
        for algo in data:
            content = addItem(folder,
                              title=algo['title'],
                              id=algo['id'],
                              portal_type='org.bccvl.content.function',
                              method=algo['method'])
        transaction.commit()
    app._p_jar.sync()


def add_enviro_data(app, data):
    portal = app.unrestrictedTraverse('bccvl')
    # set plone site as current site to enable local utility lookup
    with site(portal):
        portal.setupCurrentSkin(app.REQUEST)
        folder = portal.unrestrictedTraverse('{}/{}'.format(bccvldefaults.DATASETS_FOLDER_ID, bccvldefaults.DATASETS_ENVIRONMENTAL_FOLDER_ID))
        for item in data:
            tmp_dir = mkdtemp(dir=Globals.data_dir)
            zipfile = os.path.join(Globals.data_dir,
                                   os.path.basename(item['url']))
            zipname =  os.path.basename(item['url'])
            content = addItem(folder, title=item['title'],
                              id=os.path.splitext(zipname)[0].encode('utf-8'))
            if item['type'] == 'file':
                # create copy of file, otherwise it may be that ZODB.Blob consumeFile
                # just moves the file (effectively deleting it at source location)
                destzip = os.path.join(tmp_dir, zipname)
                shutil.copyfile(zipfile,  destzip)
                contentzip = addDataset(content,
                                        filename=destzip,
                                        mimetype='application/zip')
            elif item['type'] == 'link':
                contentzip = addLink(content, item['url'])
            cgraph = IGraph(contentzip)
            cgraph.add((cgraph.identifier, BCCPROP['datagenre'],
                        item['genre']))
            # TODO: attach proper metadat to files (probably needs inspection of zip to find out layers and filenames)
            rdfhandler = getUtility(IORDF).getHandler()
            cc = rdfhandler.context(user='Importer',
                                    reason="auto import content")
            # store modified data
            cc.add(cgraph)
            # send changeset
            cc.commit()

            transaction.commit()
            shutil.rmtree(tmp_dir)
    app._p_jar.sync()


def add_occurence_data(app):
    portal = app.unrestrictedTraverse('bccvl')
    # set plone site as current site to enable local utility lookup
    with site(portal):
        portal.setupCurrentSkin(app.REQUEST)
        folder = portal.unrestrictedTraverse('{}/{}'.format(bccvldefaults.DATASETS_FOLDER_ID, bccvldefaults.DATASETS_SPECIES_FOLDER_ID))
        tmp_dir = mkdtemp(dir=Globals.data_dir)
        for dirname in resource_listdir('org.bccvl.testsetup', 'data/species'):
            content = addItem(folder,
                              title=u'Occurence Data for {}'.format(dirname),
                              id=dirname.encode('utf-8'))
            for data in resource_listdir('org.bccvl.testsetup', 'data/species/' +  dirname):
                # TODO: files sholud get metadat as well
                resource = resource_stream('org.bccvl.testsetup', 'data/species/' +  dirname + '/' + data)
                # create copy of file, otherwise it may be that ZODB.Blob consumeFile
                # just moves the file (effectively deleting it at source location)
                tmpfilename = os.path.join(tmp_dir, data)
                tmpfile = open(tmpfilename, 'w')
                shutil.copyfileobj(resource,  tmpfile)
                tmpfile.close()
                contentfile = addDataset(content,
                                         filename=tmpfilename,
                                         mimetype='text/csv')
                cgraph = IGraph(contentfile)
                cgraph.add((cgraph.identifier, BCCPROP['datagenre'],
                            BCCVOCAB['DataGenreSO']))
                if 'occur' in data:
                    cgraph.add((cgraph.identifier, BCCPROP['specieslayer'],
                                BCCVOCAB['SpeciesLayerP']))
                elif 'bkgd' in data:
                    cgraph.add((cgraph.identifier, BCCPROP['specieslayer'],
                                BCCVOCAB['SpeciesLayerX']))
                rdfhandler = getUtility(IORDF).getHandler()
                cc = rdfhandler.context(user='Importer',
                                        reason="auto import content")
                # store modified data
                cc.add(cgraph)
                # send changeset
                cc.commit()

            transaction.commit()
        shutil.rmtree(tmp_dir)
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
            cmd = ['gdal_translate', '-of', 'GTiff', ascfile,  tfile]
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

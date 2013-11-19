from zope.interface import implementer, provider
from collective.transmogrifier.interfaces import ISectionBlueprint
from collective.transmogrifier.interfaces import ISection
from collective.transmogrifier.utils import defaultMatcher
import urllib
import glob
import os
import os.path
import shutil
import re
import logging
from gu.z3cform.rdf.interfaces import IORDF, IGraph
from zope.component import getUtility
from org.bccvl.site.namespace import BCCPROP, BCCVOCAB
from tempfile import mkstemp, mkdtemp
from zipfile import ZipFile, ZIP_DEFLATED
import subprocess
from copy import deepcopy
from cStringIO import StringIO
import Globals
from rdflib import Namespace, Literal, Graph, RDF
from ordf.namespace import DC
NFO = Namespace(u'http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#')
BIOCLIM = Namespace(u'http://namespaces.bccvl.org.au/bioclim#')

LOG = logging.getLogger(__name__)

# def download_section(....):

#     go through item['file']:
#       if 'url':
#            download file, convert to tif,
#            rezip,  store in cache,
#            set 'filename' and load contents int '_file'
#       if download file contains more folders:
#           and they have ..._20xx / bioclim_...asc pattern
#           then yield more than one item frome here
#           update year in metadata


@provider(ISectionBlueprint)
@implementer(ISection)
class DownloadFile(object):

    def __init__(self, transmogrifier, name, options, previous):
        self.transmogrifier = transmogrifier
        self.name = name
        self.options = options
        self.previous = previous

        # TODO: Need configurable cache folder?

        # keys for sections further down the chain
        self.pathkey = options.get('path-key', '_path').strip()
        self.fileskey = options.get('files-key', '_files').strip()

    def __iter__(self):
        for item in self.previous:
            # check if current item has 'file'
            LOG.info("Check for downloads %s", item['_path'])
            if 'file' not in item:
                yield item
                continue
            if item['file'].get('contenttype') != 'application/zip':
                yield item
                continue
            # do we have a 'url' to fetch?
            if 'url' in item['file']:
                self.downloadData(item)
            # TODO: check if this is really a zip before we proceed
            # extract zip
            # create a zip for each folder and yield the item
            tmpdir = self.convertToGTiff(item)
            for folder in os.listdir(tmpdir):
                if not os.path.isdir(os.path.join(tmpdir, folder)):
                    # should not happen with current data
                    continue
                newitem = deepcopy(item)
                # Update Item data
                self.updateItemData(newitem, tmpdir, folder)
                LOG.info("Yield new item %s", newitem['_path'])
                yield newitem
            shutil.rmtree(tmpdir)

    def downloadData(self, item):
        #tmp_dir = mkdtemp(dir=Globals.data_dir)
        zipdir = os.path.join(Globals.data_dir, 'testsetup')
        if not os.path.isdir(zipdir):
            os.mkdir(zipdir)
        zipname = os.path.basename(item['file']['url'])
        zipfile = os.path.join(Globals.data_dir,
                               'testsetup',
                               zipname)
        if not os.path.exists(zipfile):
            urllib.urlretrieve(item['file']['url'], zipfile)
        # We have the file now, let's replace 'url' with 'file'
        del item['file']['url']
        item['file']['file'] = zipfile

    def convertToGTiff(self, item):
        tmpdir = mkdtemp(dir=Globals.data_dir)
        zipfile = item['file']['file']
        if zipfile in item['_files']:
            zipfile = StringIO(item['_files'][zipfile]['data'])
        with ZipFile(zipfile, 'r') as zip:
            zip.extractall(path=tmpdir)
        # gunzip all .asc.gz files
        gzglob = glob.glob(os.path.join(tmpdir, '*', '*.gz'))
        if gzglob:
            cmd = ['gunzip']
            cmd.extend(gzglob)
            ret = subprocess.call(cmd)
            if ret != 0:
                LOG.fatal('Uncompressing asc files for %s failed', zipfile)
                raise Exception('Uncompressing asc files for %s failed', zipfile)
        # convert all files to geotiff
        for ascfile in glob.glob(os.path.join(tmpdir, '*', '*.asc')):
            tfile, _ = os.path.splitext(ascfile)
            tfile += '.tif'
            cmd = ['gdal_translate', '-of', 'GTiff', ascfile,  tfile]
            ret = subprocess.call(cmd)
            if ret != 0:
                LOG.fatal('Conversion to GeoTiff for %s failed', ascfile)
                raise Exception('Conversion to GeoTiff for %s failed', ascfile)
        return tmpdir

    def getArchiveItem(self, filename, zipfilename):
        item = {
            'filename': zipfilename,
            'filesize': str(os.path.getsize(filename))
            }
        match = re.match(r'.*bioclim_(\d\d).tif', zipfilename)
        if match:
            bid = match.group(1)
            item['bioclim'] = BIOCLIM['B' + bid]
        return item

    def updateItemData(self, item, tmpdir, folder):
        '''
        item: current metadata
        folder: folder name of to zip data
        '''
        # extract year
        year = None
        match = re.match(r'.*(\d\d\d\d)$', folder)
        if match:
            year = match.group(1)
            rdffile = item.get('_rdf', {}).get('file')
            if rdffile:
                graph = Graph()
                graph.parse(data=item['_files'][rdffile]['data'], format='turtle')
                graph.add((graph.identifier, DC['temporal'], Literal("start=%s; end=%s; scheme=W3C-DTF" % (year, year), datatype=DC['Period'])))
                item['_files'][rdffile]['data'] = graph.serialize(format='turtle')
        # update item title as well
        if year:
            item['title'] = ' - '.join((item['title'], year))
        # update item path
        item['_path'] = os.path.dirname(item['_path']) + "/" + folder
        # update file data and contents
        #del item['file']['url']  # remove download location
        item['file']['file'] = folder + '.zip'  # set new filename
        newfile = StringIO()
        item['_archiveitems'] = []
        # zip content and add to item['_files']
        with ZipFile(newfile, 'w', ZIP_DEFLATED) as newzip:
            for filename in sorted(glob.glob(os.path.join(tmpdir,  folder,  '*.tif'))):
                _, dirname = os.path.split(os.path.dirname(filename))
                zipfilename = os.path.join(dirname, os.path.basename(filename))
                newzip.write(filename, zipfilename)
                # add _archiveitems metadata for ArchiveItemRDF blueprint
                item['_archiveitems'].append(self.getArchiveItem(filename, zipfilename))
        item['_files'][folder + '.zip'] = {
            'data': newfile.getvalue()
            }


@provider(ISectionBlueprint)
@implementer(ISection)
class ArchiveItemRDF(object):

    def __init__(self, transmogrifier, name, options, previous):
        self.transmogrifier = transmogrifier
        self.name = name
        self.options = options
        self.previous = previous
        self.context = transmogrifier.context

        # keys for sections further down the chain
        self.pathkey = defaultMatcher(options, 'path-key', name, 'path')
        self.fileskey = options.get('files-key', '_files').strip()
        self.rdfkey = options.get('rdf-key', '_rdf').strip()

    def __iter__(self):
        for item in self.previous:
            LOG.info("Check for archiveitem %s", item['_path'])
            pathkey = self.pathkey(*item.keys())[0]
            if not pathkey:
                yield item
                continue

            path = item[pathkey]
            # skip import context
            if not path:
                yield item
                continue

            obj = self.context.unrestrictedTraverse(
                path.encode().lstrip('/'), None)

            if obj is None:
                yield item
                continue

            if 'file' not in item:
                yield item
                continue

            graph = IGraph(obj)
            if (graph.value(graph.identifier, BCCPROP['datagenre'])
                not in (BCCVOCAB['DataGenreE'], BCCVOCAB['DataGenreFC'])):
                yield item
                continue

            # TODO: check if file is already in list?
            if (graph.value(graph.identifier, BCCPROP['hasArchiveItem'])):
                # We have already archiveitems.
                yield item
                continue
            # extract year
            for aitem in item['_archiveitems']:
                LOG.info("Generate archivitems for %s", item['_path'])
                #archiveinfo = getFileGraph(obj)
                ordf = getUtility(IORDF)
                agraph = Graph(identifier=ordf.generateURI())
                agraph.add((agraph.identifier, NFO['fileName'], Literal(aitem['filename'])))
                agraph.add((agraph.identifier, NFO['fileSize'], Literal(aitem['filesize'])))
                agraph.add((agraph.identifier, RDF['type'], NFO['ArchiveItem']))
                if 'bioclim' in aitem:
                    agraph.add((agraph.identifier, BIOCLIM['bioclimVariable'], aitem['bioclim']))
                graph.add((graph.identifier, BCCPROP['hasArchiveItem'], agraph.identifier))
                ordf.getHandler().put(agraph)
            ordf.getHandler().put(graph)
            yield item

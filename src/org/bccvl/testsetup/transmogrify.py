from zope.interface import implementer, provider
from collective.transmogrifier.interfaces import ISectionBlueprint
from collective.transmogrifier.interfaces import ISection
from collective.transmogrifier.utils import defaultMatcher
import urllib
import os
import os.path
import re
import logging
import shutil
from gu.z3cform.rdf.interfaces import IORDF, IGraph
from zope.component import getUtility
from org.bccvl.site.namespace import BCCPROP, BCCVOCAB, NFO, BIOCLIM
from tempfile import mkdtemp
from zipfile import ZipFile
from io import BytesIO
from rdflib import Literal, Graph, RDF
from ordf.namespace import DC

LOG = logging.getLogger(__name__)


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
        # get filters from configuration
        self.gcm = set(options.get('gcm', "").split())
        self.emsc = set(options.get('emsc', "").split())
        self.year = set(options.get('year', "").split())
        # A temporary directory to store downloaded files
        self.tmpdir = None

    def skip_item(self, item):
        if not 'url' in item['file']:
            # filter only downloadable content
            return False
        # TODO: use path-key
        itemid = item['_path'].split('/')[-1]
        if self.gcm and not any((gcm in itemid for gcm in self.gcm)):
            return True
        if self.emsc and not any((emsc in itemid for emsc in self.emsc)):
            return True
        if self.year and not any((year in itemid for year in self.year)):
            return True
        return False

    def __iter__(self):
        for item in self.previous:
            # check if current item has 'file'
            LOG.info("Check for downloads %s", item['_path'])
            if 'file' not in item:
                yield item
                continue
            # only skip items in climate layer
            if '/climate/' in item['_path'] and self.skip_item(item):
                # we are only filtering items with file
                LOG.info("Skip item %s", item['_path'])
                continue
            if item['file'].get('contenttype') != 'application/zip':
                yield item
                continue
            # do we have a 'url' to fetch?
            if 'url' in item['file']:
                self.downloadData(item)
            # TODO: check if update works
            self.updateItemData(item)
            yield item
            # clean up downloaded file
            if self.tmpdir and os.path.exists(self.tmpdir):
                LOG.info('Remove temp folder %s', self.tmpdir)
                shutil.rmtree(self.tmpdir)
                self.tmpdir = None

    def downloadData(self, item):
        self.tmpdir = mkdtemp('testsetup')
        # use basename from download url as filename
        zipname = os.path.basename(item['file']['url'])
        # covert to absolute path
        zipfile = os.path.join(self.tmpdir, zipname)
        # check if file exists (shouldn't)
        if not os.path.exists(zipfile):
            LOG.info('Download %s to %s', item['file']['url'], zipfile)
            # TODO: 3rd argument could be report hook, which is a method that
            #       need to accet 3 params: numblocks, bytes per block, total size(-1)
            urllib.urlretrieve(item['file']['url'], zipfile)
        # We have the file now, let's replace 'url' with 'file'
        del item['file']['url']
        item['file']['filename'] = zipname
        item['file']['file'] = zipfile

    def getArchiveItem(self, zipinfo):
        item = {
            'filename': zipinfo.filename,
            'filesize': zipinfo.file_size,
            }
        match = re.match(r'.*bioclim_(\d\d).tif', zipinfo.filename)
        if match:
            bid = match.group(1)
            item['bioclim'] = BIOCLIM['B' + bid]
        return item

    def updateItemData(self, item):
        '''
        item: current metadata
        folder: folder name of to zip data
        '''
        # update file data and contents
        item['_archiveitems'] = []
        # check if we have the file already or have to read it
        filename = item['file']['file']
        if filename in item['_files']:
            filedict = item['_files'][filename]
            zipfile = BytesIO(filedict['data'])
        else:
            zipfile = filename
        # read zip contents and generate archiveitems metadat
        zipfile = ZipFile(zipfile, 'r')
        for zipinfo in zipfile.infolist():
            if zipinfo.filename.endswith('/'):
                # skip directories
                continue
            # add _archiveitems metadata for ArchiveItemRDF blueprint
            item['_archiveitems'].append(self.getArchiveItem(zipinfo))
        # in case we haven't read file into item data
        if filename not in item['_files']:
            # TODO: there has to be a better way than reading the whole file in
            item['_files'][filename] = {
                'data': open(filename).read()
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
                # Don't ovverride information in it.
                # TODO: maybe try to match existing graphs with to be created?
                # TODO: or update existing graphs, or delete all archiveitems and create new ones'
                yield item
                continue
            # extract year
            for aitem in item['_archiveitems']:
                LOG.info("Generate archivitems for %s", item['_path'])
                #archiveinfo = getFileGraph(obj)
                ordf = getUtility(IORDF)
                agraph = Graph(identifier=ordf.generateURI())
                agraph.add((agraph.identifier, NFO['fileName'],
                            Literal(aitem['filename'])))
                agraph.add((agraph.identifier, NFO['fileSize'],
                            Literal(aitem['filesize'])))
                agraph.add((agraph.identifier, RDF['type'],
                            NFO['ArchiveItem']))
                if 'bioclim' in aitem:
                    agraph.add((agraph.identifier, BIOCLIM['bioclimVariable'],
                                aitem['bioclim']))
                graph.add((graph.identifier, BCCPROP['hasArchiveItem'],
                           agraph.identifier))
                ordf.getHandler().put(agraph)
            ordf.getHandler().put(graph)
            yield item

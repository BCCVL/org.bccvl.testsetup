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
from org.bccvl.site.namespace import (
    BCCPROP, BCCVOCAB, BCCEMSC, BCCGCM)
from gu.plone.rdf.namespace import CVOCAB
from tempfile import mkdtemp
from rdflib import Literal, Graph, RDF, OWL
from rdflib.resource import Resource
from ordf.namespace import DC

LOG = logging.getLogger(__name__)
# TODO: make this configurable somewhere
SWIFTROOT = 'https://swift.rc.nectar.org.au:8888/v1/AUTH_0bc40c2c2ff94a0b9404e6f960ae5677'


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
        # A temporary directory to store downloaded files
        self.tmpdir = None

    def __iter__(self):
        for item in self.previous:

            # check if current item has 'file'
            LOG.info("Check for downloads %s", item['_path'])
            if 'file' not in item and 'remoteUrl' not in item:
                yield item
                continue
            # TODO: add check for _type == some dataset type

            # do we have a 'url' to fetch?
            if 'url' in item.get('file', {}) or 'remoteUrl' in item:
                self.downloadData(item)
            # self.updateItemData(item)
            yield item
            # TODO: what happens to tmp file if there is an exception while yielded?
            # clean up downloaded file
            if self.tmpdir and os.path.exists(self.tmpdir):
                LOG.info('Remove temp folder %s', self.tmpdir)
                shutil.rmtree(self.tmpdir)
                self.tmpdir = None

    def downloadData(self, item):
        """assumes ther is either 'file' or 'remoteUrl' in item dictionary.
        but not both"""
        self.tmpdir = mkdtemp('testsetup')
        fileitem = item.get('file', {})
        url = item.get('remoteUrl') or fileitem.get('url')
        name = fileitem.get('filename')
        contenttype = fileitem.get('contenttype')
        # use basename from download url as filename
        zipname = os.path.basename(url)
        # covert to absolute path
        zipfile = os.path.join(self.tmpdir, zipname)
        # check if file exists (shouldn't)
        if not os.path.exists(zipfile):
            LOG.info('Download %s to %s', url, zipfile)
            # TODO: 3rd argument could be report hook, which is a method that
            #       accepts 3 params: numblocks, bytes per block, total size(-1)
            (_, resp) = urllib.urlretrieve(url, zipfile)
            #name = name or resp.info().headers # content-disposition?
            # TODO: other interesting headers:
            #       contentlength
            #       last-modified / date
            contenttype = contenttype or resp.get('content-type')
            # FIXME: get http response headers from resp.info().headers
            #    mix filename: item.file.filename, response, basename(url)
            #  same for content-type / mime-type
        # We have the file now, let's replace 'url' with 'file'
        if 'file' in item:
            item['file']['filename'] = name or zipname
            item['file']['file'] = zipfile
            item['file']['contenttype'] = contenttype
            files = item.setdefault(self.fileskey, {})
            files[zipfile] = {
                'filename': zipfile,
                'path': zipfile,
                # dexterity schemaupdater needs data here or it will break the pipeline
                'data': open(zipfile, mode='r')
            }
        else:
            # FIXME: need to store for remoteUrl as well
            files = item.setdefault(self.fileskey, {})
            files[url] = {
                'filename': name or zipname,
                'contenttype': contenttype,
                'path': zipfile
                # data not needed here as schemaupdater won't check this file
            }


#### Below are custom sources, to inject additional items
@provider(ISectionBlueprint)
@implementer(ISection)
class FutureClimateLayer5k(object):

    def __init__(self, transmogrifier, name, options, previous):
        self.transmogrifier = transmogrifier
        self.context = transmogrifier.context
        self.name = name
        self.options = options
        self.previous = previous

        # get filters from configuration
        self.enabled = options.get('enabled', "").lower() in ("true", "1", "on", "yes")
        self.emsc = set(options.get('emsc', "").split())
        self.gcm = set(options.get('gcm', "").split())
        self.year = set(options.get('year', "").split())

    def __iter__(self):
        # exhaust previous
        for item in self.previous:
            yield item

        if not self.enabled:
            return

        # Generate new items based on source
        # One way of doing it is having a hardcoded list here
        emscs = ['RCP3PD', 'RCP45', 'RCP6', 'SRESA1B']
        gcms = ['cccma-cgcm31', 'ccsr-micro32med', 'gfdl-cm20',
                'ukmo-hadcm3']
        years = ['2015', '2025', '2035', '2045', '2055',
                 '2065', '2075', '2085']
        for emsc in emscs:
            if self.emsc and emsc not in self.emsc:
                # Skip this emsc
                continue
            for gcm in gcms:
                if self.gcm and gcm not in self.gcm:
                    # skip this gcm
                    continue
                for year in years:
                    if self.year and year not in self.year:
                        # skip this year
                        continue
                    # don't skip, yield a new item
                    yield self.createItem(emsc, gcm, year)
                    # create item

    def createItem(self, emsc, gcm, year):
        g = Graph()
        r = Resource(g, g.identifier)
        r.add(RDF['type'], CVOCAB['Dataset'])
        r.add(RDF['type'], OWL['Thing'])
        r.add(BCCPROP['datagenre'], BCCVOCAB['DataGenreFC'])
        r.add(BCCPROP['resolution'], BCCVOCAB['Resolution2_5m'])
        r.add(BCCPROP['emissionscenario'], BCCEMSC[emsc])
        r.add(BCCPROP['gcm'], BCCGCM[gcm])
        r.add(DC['temporal'], Literal("start={0}; end={0}; scheme=W3C-DTF;".format(year),
                                      datatype=DC['Period']))
        url = "{0}/australia_5km/{1}_{2}_{3}.zip".format(
            SWIFTROOT, emsc, gcm, year)
        filename = os.path.basename(url)
        item = {
            "_path": 'datasets/climate/{}'.format(filename),
            "_type": "org.bccvl.content.remotedataset",
            "title": "Climate Projection {0} based on {1}, 2.5arcmin (~5km) - {2}".format(
                     emsc, gcm, year),
            "remoteUrl": url,
            "_transitions": "publish",
            "_rdf": {
                "file": "_rdf.ttl",
                "contenttype": "text/turtle"
            },
            "_files": {
                "_rdf.ttl": {
                    "data": g.serialize(format='turtle')
                }
            }
        }
        return item


@provider(ISectionBlueprint)
@implementer(ISection)
class NationalSoilgridLayers(object):

    def __init__(self, transmogrifier, name, options, previous):
        self.transmogrifier = transmogrifier
        self.context = transmogrifier.context
        self.name = name
        self.options = options
        self.previous = previous

        # get filters from configuration
        self.enabled = options.get('enabled', "").lower() in ("true", "1", "on", "yes")

    def __iter__(self):
        # exhaust previous
        for item in self.previous:
            yield item

        if not self.enabled:
            return
        # TODO: maybe put some info in here? to access in a later stage...
        #       bccvlmetadata.json may be an option here
        opt = {
            'id': 'nsg-2011-250m.zip',
            'url': '{0}/national_soil_grids/nsg-2011-250m.zip'.format(SWIFTROOT)
        }
        item = {
            "_path": 'datasets/environmental/{}'.format(opt['id']),
            "_type": "org.bccvl.content.remotedataset",
            "title": "National Soil Grids",
            "remoteUrl": opt['url'],
            "_transitions": "publish",
        }
        yield item


@provider(ISectionBlueprint)
@implementer(ISection)
class VegetationAssetsStatesTransitionsLayers(object):

    def __init__(self, transmogrifier, name, options, previous):
        self.transmogrifier = transmogrifier
        self.context = transmogrifier.context
        self.name = name
        self.options = options
        self.previous = previous

        # get filters from configuration
        self.enabled = options.get('enabled', "").lower() in ("true", "1", "on", "yes")

    def __iter__(self):
        # exhaust previous
        for item in self.previous:
            yield item

        if not self.enabled:
            return

        # TODO: maybe put some info in here? to access in a later stage...
        #       bccvlmetadata.json may be an option here
        opt = {
            'id': 'vast.zip',
            'url': '{0}/vast/vast.zip'.format(SWIFTROOT),
        }
        item = {
            "_path": 'datasets/environmental/{}'.format(opt['id']),
            "_type": "org.bccvl.content.remotedataset",
            "title": "National Scale Vegetation Assets, States and Transitions (VAST Version 2) - 2008",
            "remoteUrl": opt['url'],
            "_transitions": "publish",
        }
        yield item


@provider(ISectionBlueprint)
@implementer(ISection)
class MultiResolutionRidgeTopFlatnessLayers(object):

    def __init__(self, transmogrifier, name, options, previous):
        self.transmogrifier = transmogrifier
        self.context = transmogrifier.context
        self.name = name
        self.options = options
        self.previous = previous

        # get filters from configuration
        self.enabled = options.get('enabled', "").lower() in ("true", "1", "on", "yes")

    def __iter__(self):
        # exhaust previous
        for item in self.previous:
            yield item

        if not self.enabled:
            return

        # TODO: maybe put some info in here? to access in a later stage...
        #       bccvlmetadata.json may be an option here
        opt = {
            'id': 'multi_res_ridge_top_flat.zip',
            'url': '{0}/multi_res_ridge_top_flat/multi_res_ridge_top_flat.zip'.format(SWIFTROOT),
        }
        item = {
            "_path": 'datasets/environmental/{}'.format(opt['id']),
            "_type": "org.bccvl.content.remotedataset",
            "title": "Multi-resolution Ridge Top Flatness (MrRTF, 3\" resolution)",
            "remoteUrl": opt['url'],
            "_transitions": "publish",
        }
        yield item


@provider(ISectionBlueprint)
@implementer(ISection)
class MultiResolutionValleyBottomFlatnessLayers(object):

    def __init__(self, transmogrifier, name, options, previous):
        self.transmogrifier = transmogrifier
        self.context = transmogrifier.context
        self.name = name
        self.options = options
        self.previous = previous

        # get filters from configuration
        self.enabled = options.get('enabled', "").lower() in ("true", "1", "on", "yes")

    def __iter__(self):
        # exhaust previous
        for item in self.previous:
            yield item

        if not self.enabled:
            return

        # TODO: maybe put some info in here? to access in a later stage...
        #       bccvlmetadata.json may be an option here
        opt = {
            'id': 'multi_res_valley_bottom_flat.zip',
            'url': '{0}/multi_res_valley_bottom_flat/multi_res_valley_bottom_flat.zip'.format(SWIFTROOT),
        }
        item = {
            "_path": 'datasets/environmental/{}'.format(opt['id']),
            "_type": "org.bccvl.content.remotedataset",
            "title": "Multi-resolution Valley Bottom Flatness (MrVBF, 3\" resolution)",
            "remoteUrl": opt['url'],
            "_transitions": "publish",
        }
        yield item

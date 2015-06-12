from zope.interface import implementer, provider
from collective.transmogrifier.interfaces import ISectionBlueprint
from collective.transmogrifier.interfaces import ISection
from collective.transmogrifier.utils import defaultMatcher
from itertools import product
import urllib
import os
import os.path
import re
import logging
import shutil
from tempfile import mkdtemp

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
        """assumes there is either 'file' or 'remoteUrl' in item dictionary.
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

    resolution = 'Resolution2_5m'
    folder = 'australia_5km'
    titletempl = "Climate Projection {0} based on {1}, 2.5arcmin (~5km) - {2}"
    current_title = "Current Climate 1976 to 2005, 2.5arcmin (~5km)"
    current_file = "current.zip"

    def __init__(self, transmogrifier, name, options, previous):
        self.transmogrifier = transmogrifier
        self.context = transmogrifier.context
        self.name = name
        self.options = options
        self.previous = previous

        # get filters from configuration
        self.enabled = options.get('enabled', "").lower() in ("true", "1", "on", "yes")
        self.emsc = set(x.strip() for x in options.get('emsc', "").split(',') if x)
        self.gcm = set(x.strip() for x in options.get('gcm', "").split(',') if x)
        self.year = set(x.strip() for x in options.get('year', "").split(',') if x)

    def __iter__(self):
        # exhaust previous
        for item in self.previous:
            yield item

        if not self.enabled:
            return

        # Generate new items based on source
        # One way of doing it is having a hardcoded list here
        emscs = ['RCP3PD', 'RCP45', 'RCP6', 'RCP85',
                 'SRESA1B', 'SRESA1FI', 'SRESA2', 'SRESB1', 'SRESB2']
        gcms = ['cccma-cgcm31', 'ccsr-miroc32hi', 'ccsr-miroc32med',
                'cnrm-cm3', 'csiro-mk30', 'gfdl-cm20', 'gfdl-cm21',
                'giss-modeleh', 'giss-modeler', 'iap-fgoals10g', 'inm-cm30',
                'ipsl-cm4', 'mpi-echam5', 'mri-cgcm232a', 'ncar-ccsm30',
                'ncar-pcm1', 'ukmo-hadcm3', 'ukmo-hadgem1']
        years = ['2015', '2025', '2035', '2045', '2055',
                 '2065', '2075', '2085']
        for emsc, gcm, year in product(emscs, gcms, years):
            if self.emsc and emsc not in self.emsc:
                # Skip this emsc
                continue
            if self.gcm and gcm not in self.gcm:
                # skip this gcm
                continue
            if self.year and year not in self.year:
                # skip this year
                continue
            # don't skip, yield a new item
            yield self.createItem(emsc, gcm, year)
            # create item
        # yield current as well
        if self.current_file and (not self.year or 'current' in self.year):
            yield self.createCurrentItem()

    def createCurrentItem(self):
        item = {
            "_path": "datasets/climate/{0}/{1}".format(self.folder, self.current_file),
            "_owner": (1, 'admin'),
            "_type": "org.bccvl.content.remotedataset",
            "title": self.current_title,
            "remoteUrl": "{0}/{1}/{2}".format(SWIFTROOT, self.folder, self.current_file),
            "creators": "BCCVL",
            "_transitions": "publish",
            "bccvlmetadata": {
                "genre": "DataGenreCC",
                "resolution": self.resolution,
                "temporal": "start=1976; end=2005; scheme=W3C-DTF;"
            },
        }
        return item

    def createItem(self, emsc, gcm, year):
        url = "{0}/{1}/{2}_{3}_{4}.zip".format(
            SWIFTROOT, self.folder, emsc, gcm, year)
        filename = os.path.basename(url)
        item = {
            "_path": 'datasets/climate/{0}/{1}'.format(self.folder, filename),
            "_owner":  (1,  'admin'),
            "_type": "org.bccvl.content.remotedataset",
            "title": self.titletempl.format(
                emsc, gcm.upper(), year),
            "remoteUrl": url,
            "creators": 'BCCVL',
            "_transitions": "publish",
            "bccvlmetadata": {
                "genre": "DataGenreFC",
                "resolution": self.resolution,
                "emsc": emsc,
                "gcm": gcm,
                "temporal": "start={0}; end={0}; scheme=W3C-DTF;".format(year),
            }
        }
        return item


@provider(ISectionBlueprint)
@implementer(ISection)
class FutureClimateLayer1k(FutureClimateLayer5k):

    resolution = 'Resolution30s'
    folder = 'australia_1km'
    titletempl = "Climate Projection {0} based on {1}, 30arcsec (~1km) - {2}"
    current_title = "Current Climate 1976 to 2005, 30arcsec (~1km)"
    current_file = "current.76to05.zip"


@provider(ISectionBlueprint)
@implementer(ISection)
class FutureClimateLayer250m(FutureClimateLayer5k):

    resolution = 'Resolution9s'
    folder = 'australia_250m'
    titletempl = "Climate Projection {0} based on {1}, 9arcsec (~250m) - {2}"
    current_title = "Current Climate 1976 to 2005, 9arcsec (~250m)"
    current_file = None
    

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
            "_owner":  (1,  'admin'),
            "_type": "org.bccvl.content.remotedataset",
            "title": "National Soil Grids",
            "remoteUrl": opt['url'],
            "creators": 'BCCVL',
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
            "_owner":  (1,  'admin'),
            "_type": "org.bccvl.content.remotedataset",
            "title": "National Scale Vegetation Assets, States and Transitions (VAST Version 2) - 2008",
            "remoteUrl": opt['url'],
            "creators": 'BCCVL',
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
            "_owner":  (1,  'admin'),
            "_type": "org.bccvl.content.remotedataset",
            "title": "Multi-resolution Ridge Top Flatness (MrRTF, 3\" resolution)",
            "remoteUrl": opt['url'],
            "creators": 'BCCVL',
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
            "_owner":  (1,  'admin'),
            "_type": "org.bccvl.content.remotedataset",
            "title": "Multi-resolution Valley Bottom Flatness (MrVBF, 3\" resolution)",
            "remoteUrl": opt['url'],
            "creators": 'BCCVL',
            "_transitions": "publish",
        }
        yield item


@provider(ISectionBlueprint)
@implementer(ISection)
class AWAPLayers(object):
    """Australian Water availability project

    """

    def __init__(self, transmogrifier, name, options, previous):
        self.transmogrifier = transmogrifier
        self.context = transmogrifier.context
        self.name = name
        self.options = options
        self.previous = previous

        # get filters from configuration
        self.enabled = options.get('enabled', "").lower() in ("true", "1", "on", "yes")
        self.year = set(x.strip() for x in options.get('year', "").split(',') if x)

    def __iter__(self):
        # exhaust previous
        for item in self.previous:
            yield item

        if not self.enabled:
            return

        # datasets for years 1900 to 2011
        for year in range(1900, 2011):
            if self.year and str(year) not in self.year:
                # skip item if not selected
                continue

            # TODO: maybe put some info in here? to access in a later stage...
            #       bccvlmetadata.json may be an option here
            opt = {
                'id': 'awap_ann_{0}1231.zip'.format(year),
                'url': '{0}/awap/awap_ann_{1}1231.zip'.format(SWIFTROOT, year),
            }
            item = {
                "_path": 'datasets/environmental/{0}'.format(opt['id']),
                "_owner":  (1,  'admin'),
                "_type": "org.bccvl.content.remotedataset",
                "title": "Local Discharge (Runoff+Drainage) {0}".format(year),
                "remoteUrl": opt['url'],
                "creators": 'BCCVL',
                "_transitions": "publish",
            }
            yield item


@provider(ISectionBlueprint)
@implementer(ISection)
class GlobPETAridLayers(object):
    """Global PET and Aridity

    """

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
            'id': 'global-pet-and-aridity.zip',
            'url': '{0}/glob_pet_and_aridity/global-pet-and-aridity.zip'.format(SWIFTROOT),
        }
        item = {
            "_path": 'datasets/environmental/{0}'.format(opt['id']),
            "_owner":  (1,  'admin'),
            "_type": "org.bccvl.content.remotedataset",
            "title": "Global PET and Aridity",
            "remoteUrl": opt['url'],
            "creators": 'BCCVL',
            "_transitions": "publish",
        }
        yield item


@provider(ISectionBlueprint)
@implementer(ISection)
class NDLCLayers(object):
    """National Dynamic Land Cover datasets

    """

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

        for filename, title in (
                ('ndlc_DLCDv1_Class.zip',
                 'Dynamic Land Cover Dataset (DLCD) v1'),
                ('ndlc_trend_evi_min.zip',
                 'Trend in the annual minimum of the Enhanced Vegetation Index'),
                ('ndlc_trend_evi_mean.zip',
                 'Trend in the annual mean of the Enhanced Vegetation Index'),
                ('ndlc_trend_evi_max.zip',
                 'Trend in the annual maximum of the Enhanced Vegetation Index')):

            # TODO: maybe put some info in here? to access in a later stage...
            #       bccvlmetadata.json may be an option here
            opt = {
                'id': filename,
                'url': '{0}/national-dynamic-land-cover/{1}'.format(SWIFTROOT, filename),
            }
            item = {
                "_path": 'datasets/environmental/{0}'.format(opt['id']),
                "_owner":  (1,  'admin'),
                "_type": "org.bccvl.content.remotedataset",
                "title": title,
                "remoteUrl": opt['url'],
                "creators": 'BCCVL',
                "_transitions": "publish",
            }
            yield item

#

class WorldClimLayer(object):

    def __init__(self, transmogrifier, name, options, previous):
        self.transmogrifier = transmogrifier
        self.context = transmogrifier.context
        self.name = name
        self.options = options
        self.previous = previous

        # get filters from configuration
        self.enabled = options.get('enabled', "").lower() in ("true", "1", "on", "yes")

        
@provider(ISectionBlueprint)
@implementer(ISection)
class WorldClimFutureLayers(WorldClimLayer):

    def datasets(self):
        MODELS = {
            'ACCESS1-0': ['RCP4.5', 'RCP8.5'],
            'BCC-CSM1-1': ['RCP3PD', 'RCP4.5', 'RCP6', 'RCP8.5'],
            'CCSM4': ['RCP3PD', 'RCP4.5', 'RCP6', 'RCP8.5'],
            'CESM1-CAM5-1-FV2': ['RCP4.5'],
            'CNRM-CM5': ['RCP3PD', 'RCP4.5', 'RCP8.5'],
            'GFDL-CM3': ['RCP3PD', 'RCP4.5', 'RCP8.5'],
            'GFDL-ESM2G': ['RCP3PD', 'RCP4.5', 'RCP6'],
            'GISS-E2-R': ['RCP3PD', 'RCP4.5', 'RCP6', 'RCP8.5'],
            'HadGEM2-A0': ['RCP3PD', 'RCP4.5', 'RCP6', 'RCP8.5'],
            'HadGEM2-CC': ['RCP4.5', 'RCP8.5'],
            'HadGEM2-ES': ['RCP3PD', 'RCP4.5', 'RCP6', 'RCP8.5'],
            'INMCM4': ['RCP4.5', 'RCP8.5'],
            'IPSL-CM5A-LR': ['RCP3PD', 'RCP4.5', 'RCP6', 'RCP8.5'],
            'MIROC-ESM-CHEM': ['RCP3PD', 'RCP4.5', 'RCP6', 'RCP8.5'],
            'MIROC-ESM': ['RCP3PD', 'RCP4.5', 'RCP6', 'RCP8.5'],
            'MIROC5': ['RCP3PD', 'RCP4.5', 'RCP6', 'RCP8.5'],
            'MPI-ESM-LR': ['RCP3PD', 'RCP4.5', 'RCP8.5'],
            'MRI-CGCM3': ['RCP3PD', 'RCP4.5', 'RCP6', 'RCP8.5'],
            'NorESM1-M': ['RCP3PD', 'RCP4.5', 'RCP6', 'RCP8.5'],
        }
        YEARS = [ '2050', '2070' ]
        RESOS = [
            '5m', '10m', '2.5m', # '30s' # TODO: 30s are 12+GB, need to resolve
        ]

        for gcm, year, res in product(MODELS, YEARS, RESOS):
            for emsc in MODELS[gcm]:
                filename = '{gcm}_{emsc}_{year}_{res}.zip'.format(**locals())
                title = u'WorldClim Future Projection using {gcm} {emsc} at {res} ({year})'.format(**locals())
                if emsc == 'ccsm4':
                    emsc = 'ncar-ccsm40'
                yield filename, title, res.replace('.', '_'), year, gcm.lower(), emsc.replace('.','')
        
    def __iter__(self):
        # exhaust previous
        for item in self.previous:
            yield item

        if not self.enabled:
            return

        for filename, title, res, year, gcm, emsc  in self.datasets():
            opt = {
                'id': filename,
                'url': '{0}/worldclim/{1}'.format(SWIFTROOT, filename),
            }
            item = {
                '_path': 'datasets/climate/worldclim/{}/{}'.format(res, opt['id']),
                "_owner":  (1,  'admin'),
                "_type": "org.bccvl.content.remotedataset",
                "title": title,
                "remoteUrl": opt['url'],
                "creators": 'BCCVL',
                "_transitions": "publish",
                "bccvlmetadata": {
                    "genre": "DataGenreFC",
                    "resolution": 'Resolution{}'.format(res),
                    "emsc": emsc,
                    "gcm": gcm,
                    "temporal": "start={year}; end={year}; scheme=W3C-DTF;".format(year=year)
                },
            }
            yield item

@provider(ISectionBlueprint)
@implementer(ISection)
class WorldClimCurrentLayers(WorldClimLayer):
    def __iter__(self):
        # exhaust previous
        for item in self.previous:
            yield item

        if not self.enabled:
            return

        RESOLUTION_MAP = {
            #'30s': '30 arcsec',
            '2-5m': '2.5 arcmin',
            '5m': '5 arcmin',
            '10m': '10 arcmin',
        }

        for scale in RESOLUTION_MAP.keys():
            filename = 'worldclim_{}.zip'.format(scale)
            title = u'WorldClim Current Conditions (1950-2000) at {}'.format(RESOLUTION_MAP[scale])
            res = scale.replace('-', '_')
            opt = {
                'id': filename,
                'url': '{0}/worldclim/{1}'.format(SWIFTROOT, filename),
            }
            item = {
                '_path': 'datasets/climate/worldclim/{}/{}'.format(res, opt['id']),
                "_owner":  (1,  'admin'),
                "_type": "org.bccvl.content.remotedataset",
                "title": title,
                "remoteUrl": opt['url'],
                "creators": 'BCCVL',
                "_transitions": "publish",
                "bccvlmetadata": {
                    "genre": "DataGenreCC",
                    "resolution": 'Resolution{}'.format(res),
                    "temporal": "start=1950; end=2000; scheme=W3C-DTF;",
                },
            }
            yield item

#

@provider(ISectionBlueprint)
@implementer(ISection)
class GPPLayers(object):
    """Gross Primary Productivity
    """

    datasets = [
        ('gpp_maxmin_2000_2007.zip', "Gross Primary Productivity for 2000-2007 (min, max & mean)"),
        ('gpp_year_means2000_2007.zip', "Gross Primary Productivity for 2000-2007 (annual means)"),
    ]

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

        for dfile, dtitle in self.datasets:
            _url = '{0}/.../{1}'.format(SWIFTROOT, dfile)
            item = {
                "_path": 'datasets/environmental/{0}'.format(dfile),
                "_owner":  (1,  'admin'),
                "_type": "org.bccvl.content.remotedataset",
                "title": dtitle,
                "remoteUrl": _url,
                "creators": 'BCCVL',
                "_transitions": "publish",
            }
            yield item

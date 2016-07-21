from itertools import product
import logging
import os
import os.path

from collective.transmogrifier.interfaces import ISectionBlueprint
from collective.transmogrifier.interfaces import ISection
from collective.transmogrifier.utils import defaultMatcher
from plone import api
from zope.interface import implementer, provider

from org.bccvl.tasks.celery import app
from org.bccvl.tasks.plone import after_commit_task
from org.bccvl.site.job.interfaces import IJobTracker


LOG = logging.getLogger(__name__)
# FIXME: make this configurable somewhere
SWIFTROOT = 'https://swift.rc.nectar.org.au:8888/v1/AUTH_0bc40c2c2ff94a0b9404e6f960ae5677'


@provider(ISectionBlueprint)
@implementer(ISection)
class UpdateMetadata(object):
    """Trigger task to update file metadata on imported item"""
    # TODO: have option to run in process or async

    def __init__(self, transmogrifier, name, options, previous):
        self.transmogrifier = transmogrifier
        self.context = transmogrifier.context
        self.name = name
        self.options = options
        self.previous = previous

        # keys for sections further down the chain
        self.pathkey = defaultMatcher(options, 'path-key', name, 'path')
        self.siteurl = options.get('siteurl')
        sync = options.get('sync', 'False').strip().lower() not in ('false', '0')
        if sync:
            app.conf['CELERY_ALWAYS_EAGER'] = True


    def __iter__(self):
        for item in self.previous:

            if item.get("_type") not in ('org.bccvl.content.dataset',
                                         'org.bccvl.content.remotedataset'):
                # shortcut types we are not interested in
                yield item
                continue

            if not self.siteurl:
                LOG.warn("Can't run metadata update without configured site url")
                yield item
                continue

            pathkey = self.pathkey(*item.keys())[0]
            # no path .. can't do anything
            if not pathkey:
                yield item
                continue

            path = item[pathkey]
            # Skip the Plone site object itself
            if not path:
                yield item
                continue

            obj = self.context.unrestrictedTraverse(
                path.encode().lstrip('/'), None)

            # path doesn't exist
            if obj is None:
                yield item
                continue

            # get username
            member = api.user.get_current()
            # do we have a propery member?
            if member.getId():
                user = {
                    'id': member.getUserName(),
                    'email': member.getProperty('email'),
                    'fullname': member.getProperty('fullname')
                }
            else:
                # assume admin for background task
                user = {
                    'id': 'admin',
                    'email': None,
                    'fullname': None
                }

            # build download url
            # 1. get context (site) relative path
            context_path = self.context.getPhysicalPath()
            obj_path = obj.getPhysicalPath()
            obj_path = '/'.join(obj_path[len(context_path):])
            if obj.portal_type == 'org.bccvl.content.dataset':
                filename = obj.file.filename
                obj_url = '{}/{}/@@download/file/{}'.format(
                    self.siteurl, obj_path, filename)
            else:
                filename = os.path.basename(obj.remoteUrl)
                obj_url = '{}/{}/@@download/{}'.format(
                    self.siteurl, obj_path, filename)
            # apply "format" propertiy if available
            # TODO: format should be applied by Dexterity schema updater, but there is no schema
            #       that includes 'format'
            if 'format' in item:
                obj.format = item['format']
            # schedule metadata update task in process
            # FIXME: do we have obj.format already set?
            update_task = app.signature(
                "org.bccvl.tasks.datamover.tasks.update_metadata",
                kwargs={
                    'url': obj_url,
                    'filename': filename,
                    'contenttype': obj.format,
                    'context': {
                        'context': '/'.join(obj.getPhysicalPath()),
                        'user': user,
                    }
                },
                options={'immutable': True});

            after_commit_task(update_task)
            # track background job state
            jt = IJobTracker(obj)
            job = jt.new_job('TODO: generate id', 'generate taskname: update_metadata')
            job.type = obj.portal_type
            jt.set_progress('PENDING', 'Metadata update pending')

            yield item


#### Below are custom sources, to inject additional items
@provider(ISectionBlueprint)
@implementer(ISection)
class FutureClimateLayer5k(object):

    resolution = 'Resolution2_5m'
    swiftcontainer = 'australia_5km'
    folder = 'australia/australia_5km'
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
        description = "Current climate baseline of 1976 to 2005 - climate of 1990 - generated from aggregating monthly data from Australia Water Availability Project (AWAP; http://www.bom.gov.au/jsp/awap/). " \
                      "These data were then aggregated to Bioclim variables according to the methodology of WorldClim www.worldclim.org/methods. " \
                      "For the gridded Australian data sets which are 1-kilometer in resolution, the base layers (i.e. daily AWAP 5k grids) are the same as they are in the 5-kilometer resolution dataset. " \
                      "The difference is that the final product (i.e. the aggregated data in the form of a Bioclim variable) is interpolated from 5k res to 1k res."
        item = {
            "_path": "datasets/climate/{0}/{1}".format(self.folder, self.current_file),
            "_owner": (1, 'admin'),
            "_type": "org.bccvl.content.remotedataset",
            "title": self.current_title,
            "description": description,
            "remoteUrl": "{0}/{1}/{2}".format(SWIFTROOT, self.swiftcontainer, self.current_file),
            "format": "application/zip",
            "creators": "BCCVL",
            "_transitions": "publish",
            "bccvlmetadata": {
                "genre": "DataGenreCC",
                "resolution": self.resolution,
                "categories": ["current"],
            },
        }
        LOG.info('Import %s', item['title'])
        return item

    def createItem(self, emsc, gcm, year):
        url = "{0}/{1}/{2}_{3}_{4}.zip".format(
            SWIFTROOT, self.swiftcontainer, emsc, gcm, year)
        filename = os.path.basename(url)
        item = {
            "_path": 'datasets/climate/{0}/{1}'.format(self.folder, filename),
            "_owner":  (1,  'admin'),
            "_type": "org.bccvl.content.remotedataset",
            "title": self.titletempl.format(
                emsc, gcm.upper(), year),
            "remoteUrl": url,
            "format": "application/zip",
            "creators": 'BCCVL',
            "_transitions": "publish",
            "bccvlmetadata": {
                "genre": "DataGenreFC",
                "resolution": self.resolution,
                "emsc": emsc,
                "gcm": gcm,
                "year": year,
                "categories": ["future"],
            }
        }
        LOG.info('Import %s', item['title'])
        return item


@provider(ISectionBlueprint)
@implementer(ISection)
class FutureClimateLayer1k(FutureClimateLayer5k):

    resolution = 'Resolution30s'
    swiftcontainer = 'australia_1km'
    folder = 'australia/australia_1km'
    titletempl = "Climate Projection {0} based on {1}, 30arcsec (~1km) - {2}"
    current_title = "Current Climate 1976 to 2005, 30arcsec (~1km)"
    current_file = "current.76to05.zip"


@provider(ISectionBlueprint)
@implementer(ISection)
class FutureClimateLayer250m(FutureClimateLayer5k):

    resolution = 'Resolution9s'
    swiftcontainer = 'australia_250m'
    folder = 'australia/australia_250m'
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
            "_path": 'datasets/environmental/national_soil_grids/{}'.format(opt['id']),
            "_owner":  (1,  'admin'),
            "_type": "org.bccvl.content.remotedataset",
            "title": "National Soil Grids",
            "remoteUrl": opt['url'],
            "format": "application/zip",
            "creators": 'BCCVL',
            "format": "application/zip",
            "_transitions": "publish",
            "bccvlmetadata": {
                "genre": "DataGenreE",
                "resolution": 'Resolution9s',
                "categories": ["substrate"],
            },
        }
        LOG.info('Import %s', item['title'])
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
            "_path": 'datasets/environmental/vast/{}'.format(opt['id']),
            "_owner":  (1,  'admin'),
            "_type": "org.bccvl.content.remotedataset",
            "title": "National Scale Vegetation Assets, States and Transitions (VAST Version 2) - 2008",
            "remoteUrl": opt['url'],
            "format": "application/zip",
            "creators": 'BCCVL',
            "_transitions": "publish",
            "bccvlmetadata": {
                "genre": "DataGenreE",
                "resolution": 'Resolution30s',
                "categories": ["vegetation"],
            },
        }
        LOG.info('Import %s', item['title'])
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
            "_path": 'datasets/environmental/mrrtf/{}'.format(opt['id']),
            "_owner":  (1,  'admin'),
            "_type": "org.bccvl.content.remotedataset",
            "title": "Multi-resolution Ridge Top Flatness (MrRTF, 3\" resolution)",
            "remoteUrl": opt['url'],
            "format": "application/zip",
            "creators": 'BCCVL',
            "_transitions": "publish",
            "bccvlmetadata": {
                "genre": "DataGenreE",
                "resolution": 'Resolution3s',
                "categories": ["topography"],
            },
        }
        LOG.info('Import %s', item['title'])
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
            "_path": 'datasets/environmental/mrvbf/{}'.format(opt['id']),
            "_owner":  (1,  'admin'),
            "_type": "org.bccvl.content.remotedataset",
            "title": "Multi-resolution Valley Bottom Flatness (MrVBF, 3\" resolution)",
            "remoteUrl": opt['url'],
            "format": "application/zip",
            "creators": 'BCCVL',
            "_transitions": "publish",
            "bccvlmetadata": {
                "genre": "DataGenreE",
                "resolution": 'Resolution3s',
                "categories": ["topography"],
            },
        }
        LOG.info('Import %s', item['title'])
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
                "_path": 'datasets/environmental/awap/{0}'.format(opt['id']),
                "_owner":  (1,  'admin'),
                "_type": "org.bccvl.content.remotedataset",
                "title": "Local Discharge (Runoff+Drainage) {0}".format(year),
                "remoteUrl": opt['url'],
                "format": "application/zip",
                "creators": 'BCCVL',
                "_transitions": "publish",
                "bccvlmetadata": {
                    "genre": "DataGenreE",
                    "resolution": 'Resolution3m',
                    "year": year,
                    "categories": ["hydrology"],
                },
            }
            LOG.info('Import %s', item['title'])
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
            "_path": 'datasets/environmental/gpet/{0}'.format(opt['id']),
            "_owner":  (1,  'admin'),
            "_type": "org.bccvl.content.remotedataset",
            "title": "Global PET and Aridity",
            "description": "The Global-PET and Global-Aridity are both modeled using the data monthly average data (1950-2000) available from the WorldClim Global Climate Data.",
            "remoteUrl": opt['url'],
            "format": "application/zip",
            "creators": 'BCCVL',
            "_transitions": "publish",
            "bccvlmetadata": {
                "genre": "DataGenreE",
                "resolution": 'Resolution30s',
                "categories": ["hydrology"],
            }
        }
        LOG.info('Import %s', item['title'])
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
                "_path": 'datasets/environmental/ndlc/{0}'.format(opt['id']),
                "_owner":  (1,  'admin'),
                "_type": "org.bccvl.content.remotedataset",
                "title": title,
                "description": "Shows trend of EVI from 2000 to 2008",
                "remoteUrl": opt['url'],
                "format": "application/zip",
                "creators": 'BCCVL',
                "_transitions": "publish",
                "bccvlmetadata": {
                    "genre": "DataGenreE",
                    "resolution": 'Resolution9s',
                    "categories": ["landcover"],
                },
            }
            LOG.info('Import %s', item['title'])
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
        self.emsc = set(x.strip() for x in options.get('emsc', "").split(',') if x)
        self.gcm = set(x.strip() for x in options.get('gcm', "").split(',') if x)
        self.year = set(x.strip() for x in options.get('year', "").split(',') if x)


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
        RESOS = {
            # '30s': '30 arcsec', # TODO: 30s are 12+GB, need to resolve
            '2.5m': '2.5 arcmin',
            '5m': '5 arcmin',
            '10m': '10 arcmin',
        }
        LAYERS = ['bioclim', 'prec', 'tmin', 'tmax']

        for gcm, year, res, layer in product(MODELS, YEARS, RESOS, LAYERS):
            if self.gcm and gcm not in self.gcm:
                # skip this gcm
                continue
            if self.year and year not in self.year:
                # skip this year
                continue
            for emsc in MODELS[gcm]:
                if self.emsc and emsc not in self.emsc:
                    # skip
                    continue
                filename = '{}_{}_{}_{}_{}.zip'.format(gcm, emsc, year, res, layer)
                if layer == 'bioclim':
                    title = u'WorldClim Future Projection using {} {} at {} ({})'.format(gcm, emsc, RESOS[res], year)
                else:
                    title = u'WorldClim Future Projection monthly {} using {} {} at {} ({})'.format(layer, gcm, emsc, RESOS[res], year)
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
            item = self._createItem(title, filename, res, gcm, emsc, year)
            LOG.info('Import %s', item['title'])
            yield item

    def _createItem(self, title, filename, res, gcm, emsc, year):
        item = {
            '_path': 'datasets/climate/worldclim/{}/{}'.format(res, filename),
            "_owner":  (1,  'admin'),
            "_type": "org.bccvl.content.remotedataset",
            "title": title,
            "remoteUrl": '{0}/worldclim/{1}'.format(SWIFTROOT, filename),
            "format": "application/zip",
            "creators": 'BCCVL',
            "_transitions": "publish",
            "bccvlmetadata": {
                "genre": "DataGenreFC",
                "resolution": 'Resolution{}'.format(res),
                "emsc": emsc,
                "gcm": gcm,
                "year": year,
                "categories": ["future"],
            },
            "downloadable": False,
        }
        return item


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

        MONTHLY = ['prec', 'tmax', 'tmin', 'tmean']

        for scale in RESOLUTION_MAP.keys():
            # yield altitude layer
            title = u'WorldClim Altitude at {}'.format(RESOLUTION_MAP[scale])
            item = self._createItem(title, scale, 'alt')
            yield item
            # yield bioclim layer
            title = u'WorldClim Current Conditions (1950-2000) at {}'.format(RESOLUTION_MAP[scale])
            item = self._createItem(title, scale, 'bioclim')
            yield item
            # yield monthly layers
            for layer in MONTHLY:
                title = u'WorldClim Current Conditions monthly {} (1950-2000) at {}'.format(layer, RESOLUTION_MAP[scale])
                item = self._createItem(title, scale, layer)
                yield item

    def _createItem(self, title, scale, layer):
        res = scale.replace('-', '_')
        filename = 'worldclim_{}_{}.zip'.format(scale, layer)
        item = {
            '_path': 'datasets/climate/worldclim/{}/{}'.format(res, filename),
            '_owner': (1, 'admin'),
            "_type": "org.bccvl.content.remotedataset",
            "title": title,
            "description": "Bioclimatic variables generated using data from 1950 -2000",
            "remoteUrl": '{0}/worldclim/{1}'.format(SWIFTROOT, filename),
            "format": "application/zip",
            "creators": 'BCCVL',
            "_transitions": "publish",
            "bccvlmetadata": {
                "genre": "DataGenreCC",
                "resolution": 'Resolution{}'.format(res),
                "categories": ["current"],
            },
            "downloadable": False,
        }
        if layer == 'alt':
            item['bccvlmetadata']['categories'] = ['topography']
        LOG.info('Import %s', item['title'])
        return item

#

@provider(ISectionBlueprint)
@implementer(ISection)
class GPPLayers(object):
    """Gross Primary Productivity
    """

    datasets = [
        ('gpp_maxmin_2000_2007.zip', "Gross Primary Productivity for 2000-2007 (min, max & mean)"),
        ('gppyr_2000_01_molco2m2yr_m.zip', "Gross Primary Productivity for 2000 (annual mean)"),
        ('gppyr_2001_02_molco2m2yr_m.zip', "Gross Primary Productivity for 2001 (annual mean)"),
        ('gppyr_2002_03_molco2m2yr_m.zip', "Gross Primary Productivity for 2002 (annual mean)"),
        ('gppyr_2003_04_molco2m2yr_m.zip', "Gross Primary Productivity for 2003 (annual mean)"),
        ('gppyr_2004_05_molco2m2yr_m.zip', "Gross Primary Productivity for 2004 (annual mean)"),
        ('gppyr_2005_06_molco2m2yr_m.zip', "Gross Primary Productivity for 2005 (annual mean)"),
        ('gppyr_2006_07_molco2m2yr_m.zip', "Gross Primary Productivity for 2006 (annual mean)"),
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
            _url = '{0}/gpp/{1}'.format(SWIFTROOT, dfile)
            item = {
                "_path": 'datasets/environmental/gpp/{0}'.format(dfile),
                "_owner":  (1,  'admin'),
                "_type": "org.bccvl.content.remotedataset",
                "title": dtitle,
                "remoteUrl": _url,
                "format": "application/zip",
                "creators": 'BCCVL',
                "_transitions": "publish",
                "bccvlmetadata": {
                    "genre": "DataGenreE",
                    "resolution": 'Resolution9s',
                    "categories": ["vegetation"],
                },
            }
            if dfile == 'gpp_maxmin_2000_2007':
                item['description'] = "Data aggregated over period 2000 - 2007",
            else:
                item['description'] = 'Data for year {}'.format(dfile.split('_')[1])
            LOG.info('Import %s', item['title'])
            yield item


@provider(ISectionBlueprint)
@implementer(ISection)
class FPARLayers(object):
    """Fraction of Photosynthetically Active Radiation
    """

    # (year, start_month, end_month)
    ranges = [
        (2000, 04, 12),
        (2001, 01, 12),
        (2002, 01, 12),
        (2003, 01, 12),
        (2004, 01, 12),
        (2005, 01, 12),
        (2006, 01, 12),
        (2007, 01, 12),
        (2008, 01, 12),
        (2009, 01, 12),
        (2010, 01, 12),
        (2011, 01, 12),
        (2012, 01, 12),
        (2013, 01, 12),
        (2014, 01, 10),
    ]

    datasets = [
        ('fpar.01.stats.aust.zip'),
        ('fpar.02.stats.aust.zip'),
        ('fpar.03.stats.aust.zip'),
        ('fpar.04.stats.aust.zip'),
        ('fpar.05.stats.aust.zip'),
        ('fpar.06.stats.aust.zip'),
        ('fpar.07.stats.aust.zip'),
        ('fpar.08.stats.aust.zip'),
        ('fpar.09.stats.aust.zip'),
        ('fpar.10.stats.aust.zip'),
        ('fpar.11.stats.aust.zip'),
        ('fpar.12.stats.aust.zip'),
        ('fpar.2000.stats.aust.zip'),
        ('fpar.2001.stats.aust.zip'),
        ('fpar.2002.stats.aust.zip'),
        ('fpar.2003.stats.aust.zip'),
        ('fpar.2004.stats.aust.zip'),
        ('fpar.2005.stats.aust.zip'),
        ('fpar.2006.stats.aust.zip'),
        ('fpar.2007.stats.aust.zip'),
        ('fpar.2008.stats.aust.zip'),
        ('fpar.2009.stats.aust.zip'),
        ('fpar.2010.stats.aust.zip'),
        ('fpar.2011.stats.aust.zip'),
        ('fpar.2012.stats.aust.zip'),
        ('fpar.2013.stats.aust.zip'),
        ('fpar.2014.stats.aust.zip'),
        ('fpar.1999-2000.stats.aust.zip'),
        ('fpar.2000-2001.stats.aust.zip'),
        ('fpar.2001-2002.stats.aust.zip'),
        ('fpar.2002-2003.stats.aust.zip'),
        ('fpar.2003-2004.stats.aust.zip'),
        ('fpar.2004-2005.stats.aust.zip'),
        ('fpar.2005-2006.stats.aust.zip'),
        ('fpar.2006-2007.stats.aust.zip'),
        ('fpar.2007-2008.stats.aust.zip'),
        ('fpar.2008-2009.stats.aust.zip'),
        ('fpar.2009-2010.stats.aust.zip'),
        ('fpar.2010-2011.stats.aust.zip'),
        ('fpar.2011-2012.stats.aust.zip'),
        ('fpar.2012-2013.stats.aust.zip'),
        ('fpar.2013-2014.stats.aust.zip'),
        ('fpar.2014-2015.stats.aust.zip'),
        ('fpar.2000-2014.stats.aust.zip'),
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

        # Monthly data loop
        for year, start_month, end_month in self.ranges:
            for month in xrange(start_month, end_month+1):
                dfile = 'fpar.{year}.{month:02d}.aust.zip'.format(month=month, year=year)
                dtitle = 'MODIS-fPAR time series for Australia - {month} {year}'.format(month=month, year=year)
                _url = '{0}/fpar/{1}'.format(SWIFTROOT, dfile)
                item = {
                    "_path": 'datasets/environmental/fpar/{0}'.format(dfile),
                    "_owner":  (1,  'admin'),
                    "_type": "org.bccvl.content.remotedataset",
                    "title": dtitle,
                    "description": "Data for year {} and month {}".format(year, month),
                    "remoteUrl": _url,
                    "format": "application/zip",
                    "creators": 'BCCVL',
                    "_transitions": "publish",
                    "bccvlmetadata": {
                        "genre": "DataGenreE",
                        "resolution": 'Resolution9s',
                        "categories": ["vegetation"],
                    },
                }
                LOG.info('Import %s', item['title'])
                yield item

        # Summary statistics code
        for dfile in self.datasets:
            _url = '{0}/fpar/{1}'.format(SWIFTROOT, dfile)
            item = {
                "_path": 'datasets/environmental/fpar/{0}'.format(dfile),
                "_owner":  (1,  'admin'),
                "_type": "org.bccvl.content.remotedataset",
                "remoteUrl": _url,
                "format": "application/zip",
                "creators": 'BCCVL',
                "_transitions": "publish",
                "bccvlmetadata": {
                    "genre": "DataGenreE",
                    "resolution": 'Resolution9s',
                    "categories": ["vegetation"],
                },
            }
            if dfile == 'fpar.2000-2014.stats.aust.zip':
                item['title'] = 'MODIS-fPAR time series for Australia - Summary for 2000 to 2014 (Average, Minimum, Maximum)'
                item['description'] = "Data aggregated over years 2000 to 2014 (Average, Minimum, Maximum)".format(year=dfile.split(".")[1])
            # Growing year (Jul - Jun)
            elif len(dfile) == 29:
                year1 = dfile.split(".")[1].split("-")[0]
                year2 = dfile.split(".")[1].split("-")[1]
                item['title'] = 'MODIS-fPAR time series for Australia - {year1} to {year2} Growing Year (Average, Minimum, Maximum)'.format(year1=year1, year2=year2)
                item['description'] = "Data aggregated for {year1} to {year2} Growing Year (Annual Average, Minimum, Maximum)".format(year1=year1, year2=year2)
            # Calendar year (Jan - Dec)
            elif len(dfile) == 24:
                year = dfile.split(".")[1]
                item['title'] = 'MODIS-fPAR time series for Australia - {year} Calendar Year (Average, Minimum, Maximum)'.format(year=year)
                item['description'] = "Data aggregated for {year} Calendar Year (Annual Average, Minimum, Maximum)".format(year=year)
            # Long-term monthly
            elif len(dfile) == 22:
                month = dfile.split(".")[1]
                item['title'] = 'MODIS-fPAR time series for Australia - {month} (Long-term Monthly Average, Minimum, Maximum)'.format(month=month)
                item['description'] = "Data aggregated for {month} (Long-term Monthly Average, Minimum, Maximum)".format(month=month)

            LOG.info('Import %s', item['title'])
            yield item


@provider(ISectionBlueprint)
@implementer(ISection)
class CRUClimLayers(WorldClimLayer):

    def __init__(self, transmogrifier, name, options, previous):
        self.transmogrifier = transmogrifier
        self.context = transmogrifier.context
        self.name = name
        self.options = options
        self.previous = previous

        self.enabled = options.get('enabled', "").lower() in ("true", "1", "on", "yes")

    def __iter__(self):
        # exhaust previous
        for item in self.previous:
            yield item

        if not self.enabled:
            return

        yield self._createItem()

    def _createItem(self):
        res = "30m"
        filename = "cruclim_current_1976-2005.zip"
        item = {
            '_path': 'datasets/climate/cruclim/{}/{}'.format(res, filename),
            '_owner': (1, 'admin'),
            "_type": "org.bccvl.content.remotedataset",
            "title": u"Climatic Research Unit (CRU) current bioclimate maps (1976 - 2005) with global extent at 0.5 degrees resolution.",
            "description": u"A set of 19 bioclimatic variables, calculated according to the WorldClim method.  The 19 variables calculated are as follows: They are coded as follows: \nBIO1 = Annual Mean Temperature, BIO2 = Mean Diurnal Range (Mean of monthly (max temp - min temp)), BIO3 = Isothermality (BIO2/BIO7), BIO4 = Temperature Seasonality, BIO5 = Max Temperature of Warmest Month, BIO6 = Min Temperature of Coldest Month, BIO7 = Temperature Annual Range (BIO5-BIO6), BIO8 = Mean Temperature of Wettest Quarter, BIO9 = Mean Temperature of Driest Quarter, BIO10 = Mean Temperature of Warmest Quarter, BIO11 = Mean Temperature of Coldest Quarter, BIO12 = Annual Precipitation, BIO13 = Precipitation of Wettest Month, BIO14 = Precipitation of Driest Month, BIO15 = Precipitation Seasonality (Coefficient of Variation), BIO16 = Precipitation of Wettest Quarter, BIO17 = Precipitation of Driest Quarter, BIO18 = Precipitation of Warmest Quarter, BIO19 = Precipitation of Coldest Quarter.",
            "remoteUrl": '{0}/cruclim/{1}'.format(SWIFTROOT, filename),
            "format": "application/zip",
            "creators": 'BCCVL',
            "_transitions": "publish",
            "bccvlmetadata": {
                "genre": "DataGenreCC",
                "resolution": 'Resolution{}'.format(res),
                "categories": ["current"],
            },
        }
        return item

@provider(ISectionBlueprint)
@implementer(ISection)
class ACCUClimLayers(WorldClimLayer):

    def __init__(self, transmogrifier, name, options, previous):
        self.transmogrifier = transmogrifier
        self.context = transmogrifier.context
        self.name = name
        self.options = options
        self.previous = previous

        self.enabled = options.get('enabled', "").lower() in ("true", "1", "on", "yes")

    def __iter__(self):
        # exhaust previous
        #import ipdb; ipdb.set_trace()
        for item in self.previous:
            yield item

        if not self.enabled:
            return

        for year in range(1965, 2001, 5):        
            yield self._createItem(year)

    def _createItem(self, year):
        res = "9m"
        filename = 'accuclim_{year}.zip'.format(year=year)
        item = {
            '_path': 'datasets/climate/accuclim/{}/{}'.format(res, filename),
            '_owner': (1, 'admin'),
            "_type": "org.bccvl.content.remotedataset",
            "title": u'accuCLIM Bioclimate Map Time-Series, 1965 - 2000: year {year}. 30-year average mapped bioclimatic variables for the Australian Wet Tropics, statistically downscaled according to key environmental and topographic factors, at 9 arcsecond resolution.'.format(year=year),
            "description": u"A set of 7 bioclimatic variables, calculated according to the WorldClim method.  They are coded as follows: accuCLIM_01 = Annual Mean Temperature, accuCLIM_02 = Mean Diurnal Range, accuCLIM_03 = Isothermality (accuCLIM_02/accuCLIM_07), accuCLIM_04 = Temperature Seasonality, accuCLIM_05 = Max Temperature of Warmest Month, accuCLIM_06 = Min Temperature of Coldest Month, accuCLIM_07 = Temperature Annual Range (accuCLIM_05-accuCLIM_06).",
            "remoteUrl": '{0}/accuclim/{1}'.format(SWIFTROOT, filename),
            "format": "application/zip",
            "creators": 'BCCVL',
            "_transitions": "publish",
            "bccvlmetadata": {
                "genre": "DataGenreCC",
                "resolution": 'Resolution{}'.format(res),
                "categories": ["current"],
            },
        }
        LOG.info('Import %s', item['title'])
        return item

@provider(ISectionBlueprint)
@implementer(ISection)
class TASClimLayers(WorldClimLayer):

    emscs = ['SRES-A2', 'SRES-B1']
    gcms = ['ECHAM5', 'GCM_MEAN', 'GFDL-CM2.0', 'GFDL-CM2.1', 'MIROC3.2_MEDRES', 'UKMO-HadCM3']

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
        #import ipdb; ipdb.set_trace()
        for item in self.previous:
            yield item

        if not self.enabled:
            return

        for emsc in self.emscs:
            for gcm in self.gcms:
                for year in range(1980, 2086, 5):
                    yield self._createItem(emsc, gcm, year)

    def _createItem(self, emsc, gcm, year):
        res = "6m"
        filename = 'TASCLIM_{emsc}_{gcm}_{year}.zip'.format(emsc=emsc, gcm=gcm, year=year)
        item = {
            '_path': 'datasets/climate/tasclim/{}/{}'.format(res, filename),
            '_owner': (1, 'admin'),
            "_type": "org.bccvl.content.remotedataset",
            "title": u'Tasmania Future Climate ({emsc}) based on {gcm}, 6 arcmin ({year})'.format(emsc=emsc.upper(), gcm=gcm.upper(), year=year),
            "description": u"Climate Futures Tasmania (CFT) Bioclimate Map Time-Series, 1980 - 2085. A set of 19 bioclimatic variables (30-year average) with 6 arcminute resolution, calculated according to the WorldClim method.",
            "remoteUrl": '{0}/tasclim/{1}'.format(SWIFTROOT, filename),
            "format": "application/zip",
            "creators": 'BCCVL',
            "_transitions": "publish",
            "bccvlmetadata": {
                "genre": "DataGenreFC",
                "resolution": 'Resolution{}'.format(res),
                "emsc": emsc,
                "gcm": gcm,
                "year": year,
                "categories": ["future"],
            },
        }

        # Set category to current for year <= 2015
        if  year <= 2015:
            item["title"] = u'Tasmania Current Climate ({emsc}) based on {gcm}, 6 arcmin ({year})'.format(emsc=emsc.upper(), gcm=gcm.upper(), year=year)
            item["bccvlmetadata"] = {
                "genre": "DataGenreCC",
                "resolution": 'Resolution{}'.format(res),
                "emsc": emsc,
                "gcm": gcm,
                "year": year,
                "categories": ["current"],
            }
        LOG.info('Import %s', item['title'])
        return item


@provider(ISectionBlueprint)
@implementer(ISection)
class ClimondLayers(WorldClimLayer):

    emscs = ['SRES-A2', 'SRES-A1B']
    gcms = ['MIROC-H', 'CSIRO-Mk3.0']

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
        #import ipdb; ipdb.set_trace()
        for item in self.previous:
            yield item

        if not self.enabled:
            return

        # Current climate datasets
        yield self._createCurrentItem()

        # Future climate datasets
        for emsc in self.emscs:
            for gcm in self.gcms:
                for year in [2030, 2050, 2070, 2090, 2100]:
                    yield self._createItem(emsc, gcm, year)

    def _createItem(self, emsc, gcm, year):
        res = "10m"
        filename = 'CLIMOND_{emsc}_{gcm}_{year}.zip'.format(emsc=emsc, gcm=gcm, year=year)
        item = {
            '_path': 'datasets/climate/climond/{}/{}'.format(res, filename),
            '_owner': (1, 'admin'),
            "_type": "org.bccvl.content.remotedataset",
            "title": u'CliMond Future Climate ({emsc}) based on {gcm}, 10 arcmin ({year})'.format(emsc=emsc.upper(), gcm=gcm.upper(), year=year),
            "description": u"CLIMOND Bioclimate Map Time-Series, 1975 - 2100.  A set of 35 bioclimatic variables (30-year average) with 10 arcminute resolution, calculated according to the WorldClim method.",
            "remoteUrl": '{0}/climond/{1}'.format(SWIFTROOT, filename),
            "format": "application/zip",
            "creators": 'BCCVL',
            "_transitions": "publish",
            "bccvlmetadata": {
                "genre": "DataGenreFC",
                "resolution": 'Resolution{}'.format(res),
                "emsc": emsc,
                "gcm": gcm,
                "year": year,
                "categories": ["future"],
            },
        }

        LOG.info('Import %s', item['title'])
        return item

    def _createCurrentItem(self):
        res = "10m"
        filename = 'CLIMOND_CURRENT.zip'
        item = {
            '_path': 'datasets/climate/climond/{}/{}'.format(res, filename),
            '_owner': (1, 'admin'),
            "_type": "org.bccvl.content.remotedataset",
            "title": u'CliMond Current Climate, 10 arcmin (1975)',
            "description": u"CLIMOND Bioclimate Map Time-Series, 1975 - 2100.  A set of 35 bioclimatic variables (30-year average) with 10 arcminute resolution, calculated according to the WorldClim method.",
            "remoteUrl": '{0}/climond/{1}'.format(SWIFTROOT, filename),
            "format": "application/zip",
            "creators": 'BCCVL',
            "_transitions": "publish",
            "bccvlmetadata": {
                "genre": "DataGenreCC",
                "resolution": 'Resolution{}'.format(res),
                "categories": ["current"],
            },
        }

        LOG.info('Import %s', item['title'])
        return item

@provider(ISectionBlueprint)
@implementer(ISection)
class NarclimLayers(WorldClimLayer):

    emscs = ['SRES-A2']
    gcms = ['CCCMA3.1', 'CSIRO-MK3.0', 'ECHAM5', 'MIROC3.2']
    rcms = ['R1', 'R2', 'R3']

    # NaRCLIM current datasets 
    current_datasets = [('NaRCLIM_baseline_Aus_Extent.zip', '36s', 2000), ('NaRCLIM_baseline_NaR_Extent.zip', '36s', 2000), ('NaRCLIM_baseline.zip', '9s', 2000)]

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

        # Future climate datasets
        for gcm in self.gcms:
            for rcm in self.rcms:
                for year in [2030, 2070]:
                    for res in ['36s', '9s']:
                        yield self._createItem(gcm, rcm, res, year)

        # Current climate datasets
        for filename, res, year in self.current_datasets:
            yield self._createCurrentItem(filename, res, year)


    def _createItem(self, gcm, rcm, res, year):
        if res == '36s':
            resolution = '36 arcsec'
        else:
            resolution = '9 arcsec'
        filename = 'NaRCLIM_{gcm}_{rcm}_{year}.zip'.format(gcm=gcm, rcm=rcm, year=year)
        item = {
            '_path': 'datasets/climate/narclim/{}/{}'.format(res, filename),
            '_owner': (1, 'admin'),
            "_type": "org.bccvl.content.remotedataset",
            "title": u'NaRCLIM Future Climate (SRES-A2) based on {gcm}-{rcm}, {resolution} ({year})'.format(gcm=gcm.upper(), rcm=rcm.upper(), resolution=resolution, year=year),
            "description": u"NaRCLIM Bioclimate Maps: year {year}. A set of 35 bioclimatic variables (20-year average) for NSW, VIC & ACT with {resolution} resolution, calculated according to the WorldClim method.".format(year=year, resolution=resolution),
            "remoteUrl": '{0}/narclim/{1}/{2}'.format(SWIFTROOT, res, filename),
            "format": "application/zip",
            "creators": 'BCCVL',
            "_transitions": "publish",
            "bccvlmetadata": {
                "genre": "DataGenreFC",
                "resolution": 'Resolution{}'.format(res),           # This shall match to the resolution vacab in registry
                "emsc": 'SRES-A2',
                "gcm": gcm,
                "rcm": rcm,
                "year": year,
                "categories": ["future"],
            },
        }

        LOG.info('Import %s', item['title'])
        return item

    def _createCurrentItem(self, filename, res, year):
        if res == '36s':
            resolution = '36 arcsec'
        else:
            resolution = '9 arcsec'
        item = {
            '_path': 'datasets/climate/narclim/{}/{}'.format(res, filename),
            '_owner': (1, 'admin'),
            "_type": "org.bccvl.content.remotedataset",
            "title": u'NaRCLIM Current Climate, {resolution} ({year})'.format(resolution=resolution, year=year),
            "description": u"NaRCLIM Bioclimate Maps: year {year}. A set of 35 bioclimatic variables (20-year average) for NSW, VIC & ACT with {resolution} resolution, calculated according to the WorldClim method.".format(year=year, resolution=resolution),
            "remoteUrl": '{0}/narclim/{1}/{2}'.format(SWIFTROOT, res, filename),
            "format": "application/zip",
            "creators": 'BCCVL',
            "_transitions": "publish",
            "bccvlmetadata": {
                "genre": "DataGenreCC",
                "resolution": 'Resolution{}'.format(res),
                "categories": ["current"],
            },
        }

        LOG.info('Import %s', item['title'])
        return item

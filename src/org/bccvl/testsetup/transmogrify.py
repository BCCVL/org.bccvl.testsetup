from itertools import product
import logging
import os
import os.path

from collective.transmogrifier.interfaces import ISectionBlueprint
from collective.transmogrifier.interfaces import ISection
from collective.transmogrifier.utils import defaultMatcher
from plone import api
from plone.app.textfield.value import RichTextValue
from zope.annotation import IAnnotations
from zope.interface import implementer, provider
from zope.component import getUtility
from zope.schema.interfaces import IVocabularyFactory

from org.bccvl.tasks.celery import app
from org.bccvl.tasks.plone import after_commit_task
from org.bccvl.site.job.interfaces import IJobTracker


LOG = logging.getLogger(__name__)
# FIXME: make this configurable somewhere
SWIFTROOT = 'https://swift.rc.nectar.org.au:8888/v1/AUTH_0bc40c2c2ff94a0b9404e6f960ae5677'

# Tag used for summary datasets
MONTHLY_DATASET_TAG = 'Monthly datasets'
FRESHWATER_DATASET_TAG = 'Freshwater datasets'
TERRESTRIAL_DATASET_TAG = 'Terrestrial datasets'
SUMMARY_DATASET_TAG = 'Summary datasets'
MARINE_DATASET_TAG = "Marine datasets"


def emsc_title(context, emsc):
    emsc_vocab = getUtility(IVocabularyFactory, 'emsc_source')(context)
    if emsc in emsc_vocab:
        return emsc_vocab.getTerm(emsc).title
    raise Exception("Invalid key {} for emission scenario".format(emsc))


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
        sync = options.get('sync', 'False').strip(
        ).lower() not in ('false', '0')
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
            # TODO: format should be applied by Dexterity schema updater,
            #       but there is no schema that includes 'format'
            if 'format' in item:
                obj.format = item['format']
            if 'subject' in item:
                obj.subject = item['subject']
            if 'external_description' in item:
                obj.external_description = RichTextValue(item['external_description'])

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
                immutable=True)

            after_commit_task(update_task)
            # track background job state
            jt = IJobTracker(obj)
            job = jt.new_job('TODO: generate id',
                             'generate taskname: update_metadata',
                             function=obj.dataSource,
                             type=obj.portal_type)
            jt.set_progress('PENDING', 'Metadata update pending')

            yield item


# Below are custom sources, to inject additional items
@provider(ISectionBlueprint)
@implementer(ISection)
class FutureClimateLayer5k(object):

    resolution = 'Resolution2_5m'
    swiftcontainer = 'australia_5km'
    folder = 'australia/australia_5km'
    titletempl = "Australia, Climate Projection {0} based on {1}, 2.5 arcmin (~5 km) - {2}"
    current_title = "Australia, Current Climate (1976-2005), 2.5 arcmin (~5 km)"
    current_file = "current.zip"
    current_additional_tags = [SUMMARY_DATASET_TAG]

    def __init__(self, transmogrifier, name, options, previous):
        self.transmogrifier = transmogrifier
        self.context = transmogrifier.context
        self.name = name
        self.options = options
        self.previous = previous

        # get filters from configuration
        self.enabled = options.get('enabled', "").lower() in (
            "true", "1", "on", "yes")
        self.emsc = set(x.strip()
                        for x in options.get('emsc', "").split(',') if x)
        self.gcm = set(x.strip()
                       for x in options.get('gcm', "").split(',') if x)
        self.year = set(x.strip()
                        for x in options.get('year', "").split(',') if x)

    def __iter__(self):
        # exhaust previous
        for item in self.previous:
            yield item

        if not self.enabled:
            return

        # tell our event stats event handler that we collect stats later
        IAnnotations(self.context.REQUEST)['org.bccvl.site.stats.delay'] = True

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
        description = "Australia, current climate baseline of 1976 to 2005 - climate of 1990 - generated from aggregating monthly data from Australia Water Availability Project (AWAP; http://www.bom.gov.au/jsp/awap/). " \
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
            "dataSource": "ingest",
            "_transitions": "publish",
            "subject": [TERRESTRIAL_DATASET_TAG] + self.current_additional_tags,
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
            "_owner": (1, 'admin'),
            "_type": "org.bccvl.content.remotedataset",
            "title": self.titletempl.format(
                emsc_title(self.context, emsc), gcm.upper(), year),
            "remoteUrl": url,
            "format": "application/zip",
            "creators": 'BCCVL',
            "dataSource": "ingest",
            "_transitions": "publish",
            "subject": [TERRESTRIAL_DATASET_TAG],
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
    titletempl = "Australia, climate projection {0} based on {1}, 30 arcsec (~1 km) - {2}"
    current_title = "Australia, current climate (1976-2005), 30 arcsec (~1 km)"
    current_file = "current.76to05.zip"
    current_additional_tags = [SUMMARY_DATASET_TAG]


@provider(ISectionBlueprint)
@implementer(ISection)
class FutureClimateLayer250m(FutureClimateLayer5k):

    resolution = 'Resolution9s'
    swiftcontainer = 'australia_250m'
    folder = 'australia/australia_250m'
    titletempl = "Australia, Climate Projection {0} based on {1}, 9 arcsec (~250 m) - {2}"
    current_title = "Australia, current climate (1976-2005), 9 arcsec (~250 m)"
    current_file = None
    ccurrent_additional_tags = [SUMMARY_DATASET_TAG]


@provider(ISectionBlueprint)
@implementer(ISection)
class AustSubstrateFertilityLayers(object):

    def __init__(self, transmogrifier, name, options, previous):
        self.transmogrifier = transmogrifier
        self.context = transmogrifier.context
        self.name = name
        self.options = options
        self.previous = previous

        # get filters from configuration
        self.enabled = options.get('enabled', "").lower() in (
            "true", "1", "on", "yes")

    def __iter__(self):
        # exhaust previous
        for item in self.previous:
            yield item

        if not self.enabled:
            return

        # tell our event stats event handler that we collect stats later
        IAnnotations(self.context.REQUEST)['org.bccvl.site.stats.delay'] = True

        # TODO: maybe put some info in here? to access in a later stage...
        #       bccvlmetadata.json may be an option here
        opt = {
            'id': 'australian_substrate_fertility.zip',
            'url': '{0}/aust-substrate-fertility/australian_substrate_fertility.zip'.format(SWIFTROOT)
        }
        item = {
            "_path": 'datasets/environmental/aust_substrate_fertility/{}'.format(opt['id']),
            "_owner": (1, 'admin'),
            "_type": "org.bccvl.content.remotedataset",
            "title": "Australian Substrate Fertility, 36 arcsec (~1 km)",
            "remoteUrl": opt['url'],
            "creators": 'BCCVL',
            "dataSource": "ingest",
            "format": "application/zip",
            "_transitions": "publish",
            "subject": [TERRESTRIAL_DATASET_TAG],
            "bccvlmetadata": {
                "genre": "DataGenreE",
                "resolution": 'Resolution36s',
                "categories": ["substrate"],
            },
        }
        LOG.info('Import %s', item['title'])
        yield item


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
        self.enabled = options.get('enabled', "").lower() in (
            "true", "1", "on", "yes")

    def __iter__(self):
        # exhaust previous
        for item in self.previous:
            yield item

        if not self.enabled:
            return

        # tell our event stats event handler that we collect stats later
        IAnnotations(self.context.REQUEST)['org.bccvl.site.stats.delay'] = True

        # TODO: maybe put some info in here? to access in a later stage...
        #       bccvlmetadata.json may be an option here
        opt = {
            'id': 'nsg-2011-250m.zip',
            'url': '{0}/national_soil_grids/nsg-2011-250m.zip'.format(SWIFTROOT)
        }
        item = {
            "_path": 'datasets/environmental/national_soil_grids/{}'.format(opt['id']),
            "_owner": (1, 'admin'),
            "_type": "org.bccvl.content.remotedataset",
            "title": "Australia, National Soil Grids (2012), 9 arcsec (~250 m)",
            "remoteUrl": opt['url'],
            "creators": 'BCCVL',
            "dataSource": "ingest",
            "format": "application/zip",
            "_transitions": "publish",
            "subject": [TERRESTRIAL_DATASET_TAG, SUMMARY_DATASET_TAG],
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
class NationalVegetationLayers(object):

    def __init__(self, transmogrifier, name, options, previous):
        self.transmogrifier = transmogrifier
        self.context = transmogrifier.context
        self.name = name
        self.options = options
        self.previous = previous

        # get filters from configuration
        self.enabled = options.get('enabled', "").lower() in (
            "true", "1", "on", "yes")

    def __iter__(self):
        # exhaust previous
        for item in self.previous:
            yield item

        if not self.enabled:
            return

        # tell our event stats event handler that we collect stats later
        IAnnotations(self.context.REQUEST)['org.bccvl.site.stats.delay'] = True

        # TODO: maybe put some info in here? to access in a later stage...
        #       bccvlmetadata.json may be an option here
        opt = {
            'id': 'nvis_major_vegetation_groups.zip',
            'url': '{0}/nvis/nvis_major_vegetation_groups.zip'.format(SWIFTROOT)
        }
        item = {
            "_path": 'datasets/environmental/nvis_vegetation_groups/{}'.format(opt['id']),
            "_owner": (1, 'admin'),
            "_type": "org.bccvl.content.remotedataset",
            "title": "Australia, Major Vegetation Groups (2016), 3 arcsec (~90 m)",
            "remoteUrl": opt['url'],
            "creators": 'BCCVL',
            "dataSource": "ingest",
            "format": "application/zip",
            "_transitions": "publish",
            "subject": [TERRESTRIAL_DATASET_TAG],
            "bccvlmetadata": {
                "genre": "DataGenreE",
                "resolution": 'Resolution3s',
                "categories": ["vegetation"],
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
        self.enabled = options.get('enabled', "").lower() in (
            "true", "1", "on", "yes")

    def __iter__(self):
        # exhaust previous
        for item in self.previous:
            yield item

        if not self.enabled:
            return

        # tell our event stats event handler that we collect stats later
        IAnnotations(self.context.REQUEST)['org.bccvl.site.stats.delay'] = True

        # TODO: maybe put some info in here? to access in a later stage...
        #       bccvlmetadata.json may be an option here
        opt = {
            'id': 'vast.zip',
            'url': '{0}/vast/vast.zip'.format(SWIFTROOT),
        }
        item = {
            "_path": 'datasets/environmental/vast/{}'.format(opt['id']),
            "_owner": (1, 'admin'),
            "_type": "org.bccvl.content.remotedataset",
            "title": "Australia, Vegetation Assets, States and Transitions (VAST Version 2), (2008), 30 arcmin (~50 km)",
            "remoteUrl": opt['url'],
            "format": "application/zip",
            "creators": 'BCCVL',
            "dataSource": "ingest",
            "_transitions": "publish",
            "subject": [TERRESTRIAL_DATASET_TAG, SUMMARY_DATASET_TAG],
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
        self.enabled = options.get('enabled', "").lower() in (
            "true", "1", "on", "yes")

    def __iter__(self):
        # exhaust previous
        for item in self.previous:
            yield item

        if not self.enabled:
            return

        # tell our event stats event handler that we collect stats later
        IAnnotations(self.context.REQUEST)['org.bccvl.site.stats.delay'] = True

        # TODO: maybe put some info in here? to access in a later stage...
        #       bccvlmetadata.json may be an option here
        opt = {
            'id': 'multi_res_ridge_top_flat.zip',
            'url': '{0}/multi_res_ridge_top_flat/multi_res_ridge_top_flat.zip'.format(SWIFTROOT),
        }
        item = {
            "_path": 'datasets/environmental/mrrtf/{}'.format(opt['id']),
            "_owner": (1, 'admin'),
            "_type": "org.bccvl.content.remotedataset",
            "title": "Australia, Multi-resolution Ridge Top Flatness (MrRTF), (2000), 3 arcsec (~90 m)",
            "remoteUrl": opt['url'],
            "format": "application/zip",
            "creators": 'BCCVL',
            "dataSource": "ingest",
            "_transitions": "publish",
            "subject": [TERRESTRIAL_DATASET_TAG, SUMMARY_DATASET_TAG],
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
        self.enabled = options.get('enabled', "").lower() in (
            "true", "1", "on", "yes")

    def __iter__(self):
        # exhaust previous
        for item in self.previous:
            yield item

        if not self.enabled:
            return

        # tell our event stats event handler that we collect stats later
        IAnnotations(self.context.REQUEST)['org.bccvl.site.stats.delay'] = True

        # TODO: maybe put some info in here? to access in a later stage...
        #       bccvlmetadata.json may be an option here
        opt = {
            'id': 'multi_res_valley_bottom_flat.zip',
            'url': '{0}/multi_res_valley_bottom_flat/multi_res_valley_bottom_flat.zip'.format(SWIFTROOT),
        }
        item = {
            "_path": 'datasets/environmental/mrvbf/{}'.format(opt['id']),
            "_owner": (1, 'admin'),
            "_type": "org.bccvl.content.remotedataset",
            "title": "Australia, Multi-resolution Valley Bottom Flatness (MrVBF), (2000), 3 arcsec (~90 m)",
            "remoteUrl": opt['url'],
            "format": "application/zip",
            "creators": 'BCCVL',
            "dataSource": "ingest",
            "_transitions": "publish",
            "subject": [TERRESTRIAL_DATASET_TAG, SUMMARY_DATASET_TAG],
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
        self.enabled = options.get('enabled', "").lower() in (
            "true", "1", "on", "yes")
        self.year = set(x.strip()
                        for x in options.get('year', "").split(',') if x)

    def __iter__(self):
        # exhaust previous
        for item in self.previous:
            yield item

        if not self.enabled:
            return

        # tell our event stats event handler that we collect stats later
        IAnnotations(self.context.REQUEST)['org.bccvl.site.stats.delay'] = True

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
                "_owner": (1, 'admin'),
                "_type": "org.bccvl.content.remotedataset",
                "title": "Australia, Water Availability, 30 arcsec (~1 km) {0}".format(year),
                "remoteUrl": opt['url'],
                "format": "application/zip",
                "creators": 'BCCVL',
                "dataSource": "ingest",
                "_transitions": "publish",
                "subject": [TERRESTRIAL_DATASET_TAG],
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
        self.enabled = options.get('enabled', "").lower() in (
            "true", "1", "on", "yes")

    def __iter__(self):
        # exhaust previous
        for item in self.previous:
            yield item

        if not self.enabled:
            return

        # tell our event stats event handler that we collect stats later
        IAnnotations(self.context.REQUEST)['org.bccvl.site.stats.delay'] = True

        # TODO: maybe put some info in here? to access in a later stage...
        #       bccvlmetadata.json may be an option here
        opt = {
            'id': 'global-pet-and-aridity.zip',
            'url': '{0}/glob_pet_and_aridity/global-pet-and-aridity.zip'.format(SWIFTROOT),
        }
        item = {
            "_path": 'datasets/environmental/gpet/{0}'.format(opt['id']),
            "_owner": (1, 'admin'),
            "_type": "org.bccvl.content.remotedataset",
            "title": "Global, Potential Evapotranspiration and Aridity (1950-2000), 30 arsec (~1 km)",
            "description": "The Global-PET and Global-Aridity are both modeled using the data monthly average data (1950-2000) available from the WorldClim Global Climate Data.",
            "remoteUrl": opt['url'],
            "format": "application/zip",
            "creators": 'BCCVL',
            "dataSource": "ingest",
            "_transitions": "publish",
            "subject": [TERRESTRIAL_DATASET_TAG, SUMMARY_DATASET_TAG],
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
    EVI_FULL_DESC = \
        "The Enhanced Vegetation Index data is part of the Australian Dynamic Land Cover dataset. The dataset presents land cover information for every 250m by 250m area of the country from April 2000 to April 2008. " + \
        "A key aspect of land cover is vegetation greenness. The greenness of vegetation is directly related to the amount of photosynthesis occurring, and can be measured as an index such as the Enhanced Vegetation Index (EVI). " + \
        "Complete information on the creation of this product can be found in the documents available from the Geoscience Australia website: <a href=\"https://data.gov.au/dataset/f6951ba7-8968-4f64-9d38-1ed1a25785a0\" target=\"_blank\">https://data.gov.au/dataset/f6951ba7-8968-4f64-9d38-1ed1a25785a0</a> " + \
        "More information on how to interpret this data can be found here: <a href=\"http://www.ga.gov.au/scientific-topics/earth-obs/accessing-satellite-imagery/landcover/executive-summary\" target=\"_blank\">http://www.ga.gov.au/scientific-topics/earth-obs/accessing-satellite-imagery/landcover/executive-summary</a>"
    DLCD_FULL_DESC = \
        "The Dynamic Land Cover Dataset of Australia is the first nationally consistent and thematically comprehensive land cover reference for Australia. It is the result of a collaboration between Geoscience Australia and the Australian Bureau of Agriculture and Resource Economics and Sciences, and provides a base-line for identifying and reporting on change and trends in vegetation cover and extent. " + \
        "Land cover is the observed biophysical cover on the Earth's surface, including native vegetation, soils, exposed rocks and water bodies as well as anthropogenic elements such as plantations, crops and built environments. " + \
        "The dataset presents land cover information for every 250m by 250m area of the country from April 2000 to April 2008. The classification scheme used to describe land cover categories in the Dynamic Land Cover Dataset conforms to the 2007 International Standards Organisation (ISO) land cover standard (19144-2). The Dynamic Land Cover Dataset shows Australian land covers clustered into 34 ISO classes. These reflect the structural character of vegetation, ranging from cultivated and managed land covers (crops and pastures) to natural land covers such as closed forest and sparse, open grasslands. " + \
        "Complete information on the creation of this product can be found in the documents available from the Geoscience Australia website: <a href=\"https://data.gov.au/dataset/1556b944-731c-4b7f-a03e-14577c7e68db\" target=\"_blank\">https://data.gov.au/dataset/1556b944-731c-4b7f-a03e-14577c7e68db</a>"
    def __init__(self, transmogrifier, name, options, previous):
        self.transmogrifier = transmogrifier
        self.context = transmogrifier.context
        self.name = name
        self.options = options
        self.previous = previous

        # get filters from configuration
        self.enabled = options.get('enabled', "").lower() in (
            "true", "1", "on", "yes")

    def __iter__(self):
        # exhaust previous
        for item in self.previous:
            yield item

        if not self.enabled:
            return

        # tell our event stats event handler that we collect stats later
        IAnnotations(self.context.REQUEST)['org.bccvl.site.stats.delay'] = True

        for filename, addTags, title, description, full_description in (
                ('ndlc_DLCDv1_Class.zip',
                 [SUMMARY_DATASET_TAG],
                 'Australia, Dynamic Land Cover (2000-2008), 9 arcsec (~250 m)',
                 "Observed biophysical cover on the Earth's surface.",
                 self.DLCD_FULL_DESC
                ),
                ('ndlc_trend_evi.zip',
                 [SUMMARY_DATASET_TAG],
                 'Australia, Enhanced Vegetation Index (2000-2008), 9 arcsec (~250 m)',
                 "Index for the greenness of the vegetation, which is directly related to the amount of photosynthesis occurring.",
                 self.EVI_FULL_DESC
                )):

            # TODO: maybe put some info in here? to access in a later stage...
            #       bccvlmetadata.json may be an option here
            opt = {
                'id': filename,
                'url': '{0}/national-dynamic-land-cover/{1}'.format(SWIFTROOT, filename),
            }
            item = {
                "_path": 'datasets/environmental/ndlc/{0}'.format(opt['id']),
                "_owner": (1, 'admin'),
                "_type": "org.bccvl.content.remotedataset",
                "title": title,
                "description": description,
                "external_description": full_description,
                "remoteUrl": opt['url'],
                "format": "application/zip",
                "creators": 'BCCVL',
                "dataSource": "ingest",
                "_transitions": "publish",
                "subject": [TERRESTRIAL_DATASET_TAG] + addTags,
                "bccvlmetadata": {
                    "genre": "DataGenreE",
                    "resolution": 'Resolution9s',
                    "categories": ["landcover"],
                },
            }
            LOG.info('Import %s', item['title'])
            yield item

@provider(ISectionBlueprint)
@implementer(ISection)
class GlobalMarineLayers(object):
    """Global Marine datasets

    """
    COMMON_FULL_DESC = \
        "Bio-ORACLE data layers are created from monthly pre-processed satellite and in situ observations. Bio-ORACLE is developed by a team of marine researchers from the Flanders Marine Institute (VLIZ), the University of Algarve, the University of Melbourne and Ghent University. " + \
        "Website: <a href=\"http://www.bio-oracle.org/\" target=\"_blank\">http://www.bio-oracle.org/</a>"

    def __init__(self, transmogrifier, name, options, previous):
        self.transmogrifier = transmogrifier
        self.context = transmogrifier.context
        self.name = name
        self.options = options
        self.previous = previous

        # get filters from configuration
        self.enabled = options.get('enabled', "").lower() in (
            "true", "1", "on", "yes")

    def __iter__(self):
        # exhaust previous
        for item in self.previous:
            yield item

        if not self.enabled:
            return

        # tell our event stats event handler that we collect stats later
        IAnnotations(self.context.REQUEST)['org.bccvl.site.stats.delay'] = True

        for filename, category, title, description, full_description in (
                ('Present.Surface.Temperature.zip',
                 'physical',
                 'Global Marine Surface Data, Water Temperature (2000-2014), 5 arcmin (~10 km)',
                 "Global data for sea surface temperature for present time period (2000-2014).",
                 "Sea surface temperature is the temperature of the topmost meter of the ocean water column. "
                ),
                ('Present.Surface.Salinity.zip',
                 'physical',
                 'Global Marine Surface Data, Water Salinity (2000-2014), 5 arcmin (~10 km)',
                 "Global data for sea surface salinity for present time period (2000-2014).",
                 "Salinity indicates the dissolved salt content in the ocean surface. "
                ),
                ('Present.Surface.Current.Velocity.zip',
                 'physical',
                 'Global Marine Surface Data, Currents Velocity (2000-2014), 5 arcmin (~10 km)',
                 "Global data for sea surface currents velocity for present time period (2000-2014).",
                 "Measurements of current speeds at the ocean surface. "
                ),
                ('Present.Surface.Ice.thickness.zip',
                 'physical',
                 'Global Marine Surface Data, Ice Thickness (2000-2014), 5 arcmin (~10 km)',
                 "Global data for sea surface ice thickness for present time period (2000-2014).",
                 "Ice thickness in metres at the ocean surface.  "
                ),
                ('Present.Surface.Ice.cover.zip',
                 'physical',
                 'Global Marine Surface Data, Sea Ice Concentration (2000-2014), 5 arcmin (~10 km)',
                 "Global data for sea surface Sea Ice Concentration for present time period (2000-2014).",
                 "Sea ice concentration refers to the area of sea ice relative to the total area of the ocean surface. "
                ),
                ('Present.Surface.Diffuse.attenuation.zip',
                 'physical',
                 'Global Marine Surface Data, Diffuse Attenuation (2000-2014), 5 arcmin (~10 km)',
                 "Global data for sea surface diffuse attenuation for present time period (2000-2014).",
                 "The diffuse attenuation coefficient is an indicator of water clarity. It expresses how deeply visible light in the blue to the green region of the spectrum penetrates into the water column. "
                ),
                ('Present.Surface.Cloud.cover.zip',
                 'physical',
                 'Global Marine Surface Data, Cloud Cover (2000-2014), 5 arcmin (~10 km)',
                 "Global data for cloud cover for present time period (2000-2014).",
                 "Cloud cover indicates how much of the earth is covered by clouds. A bilinear interpolation was used to convert the data from 6 arcminutes to 5 arcminutes. "
                ),
                ('Present.Surface.Nitrate.zip',
                 'nutrients',
                 'Global Marine Surface Data, Nitrate (2000-2014), 5 arcmin (~10 km)',
                 "Global data for sea surface nitrate concentration for present time period (2000-2014).",
                 "Mole concentration of nitrate at the sea surface. "
                ),
                ('Present.Surface.Phosphate.zip',
                 'nutrients',
                 'Global Marine Surface Data, Phosphate (2000-2014), 5 arcmin (~10 km)',
                 "Global data for sea surface phosphate concentration for present time period (2000-2014).",
                 "Mole concentration of phosphate at the sea surface. "
                ),
                ('Present.Surface.Silicate.zip',
                 'nutrients',
                 'Global Marine Surface Data, Silicate (2000-2014), 5 arcmin (~10 km)',
                 "Global data for sea surface silicate concentration for present time period (2000-2014).",
                 "Mole concentration of silicate at the sea surface. "
                ),
                ('Present.Surface.Dissolved.oxygen.zip',
                 'nutrients',
                 'Global Marine Surface Data, Dissolved Molecular Oxygen (2000-2014), 5 arcmin (~10 km)',
                 "Global data for sea surface oxygen concentration for present time period (2000-2014).",
                 "Mole concentration of dissolved oxygen at the sea surface. "
                ),
                ('Present.Surface.Iron.zip',
                 'nutrients',
                 'Global Marine Surface Data, Iron (2000-2014), 5 arcmin (~10 km)',
                 "Global data for sea surface iron concentration for present time period (2000-2014).",
                 "Micromole concentration of dissolved iron at the sea surface. "
                ),
                ('Present.Surface.Calcite.zip',
                 'nutrients',
                 'Global Marine Surface Data, Calcite (2000-2014), 5 arcmin (~10 km)',
                 "Global data for sea surface calcite for present time period (2000-2014).",
                 "Calcite concentration indicates the mean concentration of calcite (CaCO3) in oceans. "
                ),
                ('Present.Surface.Chlorophyll.zip',
                 'biochemical',
                 'Global Marine Surface Data, Chlorophyll (2000-2014), 5 arcmin (~10 km)',
                 "Global data for sea surface chlorophyll A concentration for present time period (2000-2014).",
                 'Chlorophyll A concentration indicates the concentration of photosynthetic pigment chlorophyll A (the most common "green" chlorophyll) in oceans. Please note that in shallow water these values may reflect any kind of autotrophic biomass. '
                ),
                ('Present.Surface.Phytoplankton.zip',
                 'biochemical',
                 'Global Marine Surface Data, Phytoplankton (2000-2014), 5 arcmin (~10 km)',
                 "Global data for sea surface phytoplankton concentration for present time period (2000-2014).",
                 "Mole concentration of phytoplankton expressed as carbon at the sea surface. "
                ),
                ('Present.Surface.Primary.productivity.zip',
                 'biochemical',
                 'Global Marine Surface Data, Primary Productivity (2000-2014), 5 arcmin (~10 km)',
                 "Global data for sea surface primary productivity for present time period (2000-2014).",
                 "Sea surface net primary productivity of carbon. "
                ),
                ('Present.Surface.pH.zip',
                 'biochemical',
                 'Global Marine Surface Data, pH (2000-2014), 5 arcmin (~10 km)',
                 "Global data for sea surface pH for present time period (2000-2014).",
                 "pH is an indicator of the acidity in the ocean, with lower values indicating more acid conditions and higher values more alkaline conditions. "
                ),
                ('Present.Surface.Par.zip',
                 'biochemical',
                 'Global Marine Surface Data, Photosynthetically Available Radiation (2000-2014), 5 arcmin (~10 km)',
                 "Global data for sea surface photosynthetically available radiation for present time period (2000-2014).",
                 "Photosynthetically Available Radiation (PAR) indicates the quantum energy flux from the sun (in the spectral range 400-700 nm) reaching the ocean surface. "
                )
                ):

            # TODO: maybe put some info in here? to access in a later stage...
            #       bccvlmetadata.json may be an option here
            opt = {
                'id': filename,
                'url': '{0}/global_marine/{1}'.format(SWIFTROOT, filename),
            }
            item = {
                "_path": 'datasets/environmental/global_marine/{0}'.format(opt['id']),
                "_owner":  (1,  'admin'),
                "_type": "org.bccvl.content.remotedataset",
                "title": title,
                "description": description,
                "external_description": full_description + self.COMMON_FULL_DESC,
                "remoteUrl": opt['url'],
                "format": "application/zip",
                "creators": 'BCCVL',
                "dataSource": "ingest",
                "_transitions": "publish",
                "subject": [SUMMARY_DATASET_TAG, MARINE_DATASET_TAG],
                "bccvlmetadata": {
                    "genre": "DataGenreE",
                    "resolution": 'Resolution5m',
                    "categories": [category],
                },
            }
            LOG.info('Import %s', item['title'])
            yield item


class WorldClimLayer(object):

    def __init__(self, transmogrifier, name, options, previous):
        self.transmogrifier = transmogrifier
        self.context = transmogrifier.context
        self.name = name
        self.options = options
        self.previous = previous

        # get filters from configuration
        self.enabled = options.get('enabled', "").lower() in (
            "true", "1", "on", "yes")
        self.emsc = set(x.strip()
                        for x in options.get('emsc', "").split(',') if x)
        self.gcm = set(x.strip()
                       for x in options.get('gcm', "").split(',') if x)
        self.year = set(x.strip()
                        for x in options.get('year', "").split(',') if x)


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
        YEARS = ['2050', '2070']
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
                filename = '{}_{}_{}_{}_{}.zip'.format(
                    gcm, emsc, year, res, layer)
                monthly_tag = None
                if layer == 'bioclim':
                    title = u'WorldClim, future projection using {} {}, {} ({})'.format(
                        gcm, emsc_title(self.context, emsc.replace('.', '')), RESOS[res], year)
                else:
                    title = u'WorldClim, future projection monthly {} using {} {}, {} ({})'.format(
                        layer, gcm, emsc_title(self.context, emsc.replace('.', '')), RESOS[res], year)
                    monthly_tag = MONTHLY_DATASET_TAG
                if emsc == 'ccsm4':
                    emsc = 'ncar-ccsm40'
                yield filename, title, res.replace('.', '_'), year, gcm.lower(), emsc.replace('.', ''), monthly_tag

    def __iter__(self):
        # exhaust previous
        for item in self.previous:
            yield item

        if not self.enabled:
            return

        # tell our event stats event handler that we collect stats later
        IAnnotations(self.context.REQUEST)['org.bccvl.site.stats.delay'] = True

        for filename, title, res, year, gcm, emsc, monthtag in self.datasets():
            item = self._createItem(title, filename, res, gcm, emsc, year, monthtag)
            LOG.info('Import %s', item['title'])
            yield item

    def _createItem(self, title, filename, res, gcm, emsc, year, tag=None):
        item = {
            '_path': 'datasets/climate/worldclim/{}/{}'.format(res, filename),
            "_owner": (1, 'admin'),
            "_type": "org.bccvl.content.remotedataset",
            "title": title,
            "remoteUrl": '{0}/worldclim/{1}'.format(SWIFTROOT, filename),
            "format": "application/zip",
            "creators": 'BCCVL',
            "dataSource": "ingest",
            "_transitions": "publish",
            "subject": [TERRESTRIAL_DATASET_TAG],
            "bccvlmetadata": {
                "genre": "DataGenreFC",
                "resolution": 'Resolution{}'.format(res),
                "emsc": emsc.replace('.', ''),
                "gcm": gcm,
                "year": year,
                "categories": ["future"],
            },
            "downloadable": False,
        }
        if tag:
            item['subject'] = [tag]
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

        # tell our event stats event handler that we collect stats later
        IAnnotations(self.context.REQUEST)['org.bccvl.site.stats.delay'] = True

        RESOLUTION_MAP = {
            #'30s': '30 arcsec (~1 km)',
            '2-5m': '2.5 arcmin (~5 km)',
            '5m': '5 arcmin (~10 km)',
            '10m': '10 arcmin (~20 km)',
        }

        MONTHLY = ['prec', 'tmax', 'tmin', 'tmean']

        for scale in RESOLUTION_MAP.keys():
            # yield altitude layer
            title = u'WorldClim Altitude at {}'.format(RESOLUTION_MAP[scale])
            item = self._createItem(title, scale, 'alt')
            yield item
            # yield bioclim layer
            title = u'WorldClim, current climate (1950-2000), {}'.format(RESOLUTION_MAP[scale])
            item = self._createItem(title, scale, 'bioclim', [SUMMARY_DATASET_TAG])
            yield item
            # yield monthly layers
            for layer in MONTHLY:
                title = u'WorldClim, current climate monthly {} (1950-2000), {}'.format(layer, RESOLUTION_MAP[scale])
                item = self._createItem(title, scale, layer, [MONTHLY_DATASET_TAG])
                yield item

    def _createItem(self, title, scale, layer, addTags=[]):
        res = scale.replace('-', '_')
        filename = 'worldclim_{}_{}.zip'.format(scale, layer)
        item = {
            '_path': 'datasets/climate/worldclim/{}/{}'.format(res, filename),
            '_owner': (1, 'admin'),
            "_type": "org.bccvl.content.remotedataset",
            "title": title,
            "description": "Bioclimatic variables generated using data from 1950 to 2000",
            "remoteUrl": '{0}/worldclim/{1}'.format(SWIFTROOT, filename),
            "format": "application/zip",
            "creators": 'BCCVL',
            "dataSource": "ingest",
            "_transitions": "publish",
            "subject": [TERRESTRIAL_DATASET_TAG] + addTags,
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


@provider(ISectionBlueprint)
@implementer(ISection)
class GPPLayers(object):
    """Gross Primary Productivity
    """

    datasets = [
        ('gpp_maxmin_2000_2007.zip', [SUMMARY_DATASET_TAG],
         "Australia, Gross Primary Productivity (2000-2007) (min, max & mean), 30 arcsec (~1 km)"),
        ('gpp_summary_00_07.zip', [SUMMARY_DATASET_TAG],
         "Australia, Gross Primary Productivity (2000-2007) (coefficient of variation), 30 arcsec (~1 km)"),
        ('gppyr_2000_01_molco2m2yr_m.zip', [],
         "Australia, Gross Primary Productivity for 2000 (annual mean), 30 arcsec (~1 km)"),
        ('gppyr_2001_02_molco2m2yr_m.zip', [],
         "Australia, Gross Primary Productivity for 2001 (annual mean), 30 arcsec (~1 km)"),
        ('gppyr_2002_03_molco2m2yr_m.zip', [],
         "Australia, Gross Primary Productivity for 2002 (annual mean), 30 arcsec (~1 km)"),
        ('gppyr_2003_04_molco2m2yr_m.zip', [],
         "Australia, Gross Primary Productivity for 2003 (annual mean), 30 arcsec (~1 km)"),
        ('gppyr_2004_05_molco2m2yr_m.zip', [],
         "Australia, Gross Primary Productivity for 2004 (annual mean), 30 arcsec (~1 km)"),
        ('gppyr_2005_06_molco2m2yr_m.zip', [],
         "Australia, Gross Primary Productivity for 2005 (annual mean), 30 arcsec (~1 km)"),
        ('gppyr_2006_07_molco2m2yr_m.zip', [],
         "Australia, Gross Primary Productivity for 2006 (annual mean), 30 arcsec (~1 km)"),
    ]

    def __init__(self, transmogrifier, name, options, previous):
        self.transmogrifier = transmogrifier
        self.context = transmogrifier.context
        self.name = name
        self.options = options
        self.previous = previous

        # get filters from configuration
        self.enabled = options.get('enabled', "").lower() in (
            "true", "1", "on", "yes")

    def __iter__(self):
        # exhaust previous
        for item in self.previous:
            yield item

        if not self.enabled:
            return

        # tell our event stats event handler that we collect stats later
        IAnnotations(self.context.REQUEST)['org.bccvl.site.stats.delay'] = True

        for dfile, addTags, dtitle in self.datasets:
            _url = '{0}/gpp/{1}'.format(SWIFTROOT, dfile)
            item = {
                "_path": 'datasets/environmental/gpp/{0}'.format(dfile),
                "_owner": (1, 'admin'),
                "_type": "org.bccvl.content.remotedataset",
                "title": dtitle,
                "remoteUrl": _url,
                "format": "application/zip",
                "creators": 'BCCVL',
                "dataSource": "ingest",
                "_transitions": "publish",
                "subject": [TERRESTRIAL_DATASET_TAG] + addTags,
                "bccvlmetadata": {
                    "genre": "DataGenreE",
                    "resolution": 'Resolution9s',
                    "categories": ["vegetation"],
                },
            }

            if dfile == 'gpp_maxmin_2000_2007.zip' or 'gpp_summary_00_07.zip':
                item['description'] = "Data aggregated over period 2000 - 2007"
            elif dfile == 'gpp_summary_00_07.zip':
                item[
                    'description'] = "Data aggregated over yearly averages from 2000 - 2007"
            else:
                item['description'] = 'Data for year {}'.format(
                    dfile.split('_')[1])
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
        self.enabled = options.get('enabled', "").lower() in (
            "true", "1", "on", "yes")

    def __iter__(self):
        # exhaust previous
        for item in self.previous:
            yield item

        if not self.enabled:
            return

        # tell our event stats event handler that we collect stats later
        IAnnotations(self.context.REQUEST)['org.bccvl.site.stats.delay'] = True

        # Monthly data loop
        for year, start_month, end_month in self.ranges:
            for month in xrange(start_month, end_month + 1):
                dfile = 'fpar.{year}.{month:02d}.aust.zip'.format(
                    month=month, year=year)
                dtitle = 'Australia, MODIS-fPAR time series - {month} {year}'.format(
                    month=month, year=year)
                _url = '{0}/fpar/{1}'.format(SWIFTROOT, dfile)
                item = {
                    "_path": 'datasets/environmental/fpar/{0}'.format(dfile),
                    "_owner": (1, 'admin'),
                    "_type": "org.bccvl.content.remotedataset",
                    "title": dtitle,
                    "description": "Data for year {} and month {}".format(year, month),
                    "remoteUrl": _url,
                    "format": "application/zip",
                    "creators": 'BCCVL',
                    "dataSource": "ingest",
                    "_transitions": "publish",
                    "subject": [TERRESTRIAL_DATASET_TAG],
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
                "_owner": (1, 'admin'),
                "_type": "org.bccvl.content.remotedataset",
                "remoteUrl": _url,
                "format": "application/zip",
                "creators": 'BCCVL',
                "dataSource": "ingest",
                "_transitions": "publish",
                "subject": [TERRESTRIAL_DATASET_TAG],
                "bccvlmetadata": {
                    "genre": "DataGenreE",
                    "resolution": 'Resolution9s',
                    "categories": ["vegetation"],
                },
            }
            if dfile == 'fpar.2000-2014.stats.aust.zip':
                item['title'] = 'Australia, MODIS-fPAR time series (2000-2014), 9 arcsec (~250 m)'
                item['description'] = "Data aggregated over years 2000 to 2014 (Average, Minimum, Maximum, Coefficient of Variation)".format(
                    year=dfile.split(".")[1])
                item['subject'].append(SUMMARY_DATASET_TAG)
            # Growing year (Jul - Jun)
            elif len(dfile) == 29:
                year1 = dfile.split(".")[1].split("-")[0]
                year2 = dfile.split(".")[1].split("-")[1]
                item['title'] = 'Australia, MODIS-fPAR time series - ({year1}-{year2}) Growing Year (Average, Minimum, Maximum)'.format(
                    year1=year1, year2=year2)
                item['description'] = "Data aggregated for {year1}-{year2} Growing Year (Annual Average, Minimum, Maximum)".format(
                    year1=year1, year2=year2)
            # Calendar year (Jan - Dec)
            elif len(dfile) == 24:
                year = dfile.split(".")[1]
                item['title'] = 'Australia, MODIS-fPAR time series - ({year}) Calendar Year (Average, Minimum, Maximum)'.format(year=year)
                item['description'] = "Data aggregated for {year} Calendar Year (Annual Average, Minimum, Maximum)".format(
                    year=year)
            # Long-term monthly
            elif len(dfile) == 22:
                month = dfile.split(".")[1]
                item['title'] = 'Australia, MODIS-fPAR time series - {month} (Long-term Monthly Average, Minimum, Maximum)'.format(month=month)
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

        self.enabled = options.get('enabled', "").lower() in (
            "true", "1", "on", "yes")

    def __iter__(self):
        # exhaust previous
        for item in self.previous:
            yield item

        if not self.enabled:
            return

        # tell our event stats event handler that we collect stats later
        IAnnotations(self.context.REQUEST)['org.bccvl.site.stats.delay'] = True

        yield self._createItem()

    def _createItem(self):
        res = "30m"
        filename = "cruclim_current_1976-2005.zip"
        item = {
            '_path': 'datasets/climate/cruclim/{}/{}'.format(res, filename),
            '_owner': (1, 'admin'),
            "_type": "org.bccvl.content.remotedataset",
            "title": u"CRUclim (global), current climate (1976-2005), 30 arcmin (~50 km)",
            "description": u"A set of 19 bioclimatic variables, calculated according to the WorldClim method.  The 19 variables calculated are as follows: They are coded as follows: \nBIO1 = Annual Mean Temperature, BIO2 = Mean Diurnal Range (Mean of monthly (max temp - min temp)), BIO3 = Isothermality (BIO2/BIO7), BIO4 = Temperature Seasonality, BIO5 = Max Temperature of Warmest Month, BIO6 = Min Temperature of Coldest Month, BIO7 = Temperature Annual Range (BIO5-BIO6), BIO8 = Mean Temperature of Wettest Quarter, BIO9 = Mean Temperature of Driest Quarter, BIO10 = Mean Temperature of Warmest Quarter, BIO11 = Mean Temperature of Coldest Quarter, BIO12 = Annual Precipitation, BIO13 = Precipitation of Wettest Month, BIO14 = Precipitation of Driest Month, BIO15 = Precipitation Seasonality (Coefficient of Variation), BIO16 = Precipitation of Wettest Quarter, BIO17 = Precipitation of Driest Quarter, BIO18 = Precipitation of Warmest Quarter, BIO19 = Precipitation of Coldest Quarter.",
            "remoteUrl": '{0}/cruclim/{1}'.format(SWIFTROOT, filename),
            "format": "application/zip",
            "creators": 'BCCVL',
            "dataSource": "ingest",
            "_transitions": "publish",
            "subject": [TERRESTRIAL_DATASET_TAG, SUMMARY_DATASET_TAG],
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

        self.enabled = options.get('enabled', "").lower() in (
            "true", "1", "on", "yes")

    def __iter__(self):
        # exhaust previous
        for item in self.previous:
            yield item

        if not self.enabled:
            return

        # tell our event stats event handler that we collect stats later
        IAnnotations(self.context.REQUEST)['org.bccvl.site.stats.delay'] = True

        for year in range(1965, 2001, 5):
            yield self._createItem(year)

    def _createItem(self, year):
        res = "9m"
        filename = 'accuclim_{year}.zip'.format(year=year)
        item = {
            '_path': 'datasets/climate/accuclim/{}/{}'.format(res, filename),
            '_owner': (1, 'admin'),
            "_type": "org.bccvl.content.remotedataset",
            "title": u'accuCLIM (Wet Tropics Australia), 30-year average either side of ({year}), 9 arcsec (~250 m)'.format(year=year),
            "description": u"A set of 7 bioclimatic variables, calculated according to the WorldClim method.  They are coded as follows: accuCLIM_01 = Annual Mean Temperature, accuCLIM_02 = Mean Diurnal Range, accuCLIM_03 = Isothermality (accuCLIM_02/accuCLIM_07), accuCLIM_04 = Temperature Seasonality, accuCLIM_05 = Max Temperature of Warmest Month, accuCLIM_06 = Min Temperature of Coldest Month, accuCLIM_07 = Temperature Annual Range (accuCLIM_05-accuCLIM_06).",
            "remoteUrl": '{0}/accuclim/{1}'.format(SWIFTROOT, filename),
            "format": "application/zip",
            "creators": 'BCCVL',
            "dataSource": "ingest",
            "_transitions": "publish",
            "subject": [TERRESTRIAL_DATASET_TAG],
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

    # map emscs from file name to vorabulary id
    emscs = {
        'SRES-A2': 'SRESA2',
        'SRES-B1': 'SRESB1'
    }
    # map gcms from file name to vocab id
    gcms = {
        'ECHAM5': 'mpi-echam5',
        'GCM_MEAN': 'gcm-mean-5',
        'GFDL-CM2.0': 'gfdl-cm20',
        'GFDL-CM2.1': 'gfdl-cm21',
        'MIROC3.2_MEDRES': 'ccsr-miroc32med',
        'UKMO-HadCM3': 'ukmo-hadcm3'
    }

    def __init__(self, transmogrifier, name, options, previous):
        self.transmogrifier = transmogrifier
        self.context = transmogrifier.context
        self.name = name
        self.options = options
        self.previous = previous

        # get filters from configuration
        self.enabled = options.get('enabled', "").lower() in (
            "true", "1", "on", "yes")

    def __iter__(self):
        # exhaust previous
        for item in self.previous:
            yield item

        if not self.enabled:
            return

        # tell our event stats event handler that we collect stats later
        IAnnotations(self.context.REQUEST)['org.bccvl.site.stats.delay'] = True

        for emsc in self.emscs.keys():
            for gcm in self.gcms.keys():
                for year in range(1980, 2086, 5):
                    yield self._createItem(emsc, gcm, year)

    def _createItem(self, emsc, gcm, year):
        res = "6m"
        filename = 'TASCLIM_{emsc}_{gcm}_{year}.zip'.format(
            emsc=emsc, gcm=gcm, year=year)
        item = {
            '_path': 'datasets/climate/tasclim/{}/{}'.format(res, filename),
            '_owner': (1, 'admin'),
            "_type": "org.bccvl.content.remotedataset",
            "title": u'Tasmania, climate futures Tasmania, ({year}),  (CFT) ({emsc}) based on {gcm}, 6 arcmin (~12 km)'.format(
                emsc=emsc_title(self.context, self.emscs[emsc]), gcm=gcm.upper(), year=year),
            "description": u"Climate Futures Tasmania (CFT) Bioclimate Map Time-Series, 1980 - 2085. A set of 19 bioclimatic variables (30-year average) with 6 arcminute resolution, calculated according to the WorldClim method.",
            "remoteUrl": '{0}/tasclim/{1}'.format(SWIFTROOT, filename),
            "format": "application/zip",
            "creators": 'BCCVL',
            "dataSource": "ingest",
            "_transitions": "publish",
            "subject": [TERRESTRIAL_DATASET_TAG],
            "bccvlmetadata": {
                "genre": "DataGenreFC",
                "resolution": 'Resolution{}'.format(res),
                "emsc": self.emscs[emsc],
                "gcm": self.gcms[gcm],
                "year": year,
                "categories": ["future"],
            },
        }

        # Set category to current for year <= 2015
        if year <= 2015:
            item["title"] = u'Tasmania, Current Climate ({year}), ({emsc}) based on {gcm}, 6 arcmin (~12 km)'.format(
                emsc=emsc_title(self.context, self.emscs[emsc]), gcm=gcm.upper(), year=year)
            item["bccvlmetadata"] = {
                "genre": "DataGenreCC",
                "resolution": 'Resolution{}'.format(res),
                "emsc": self.emscs[emsc],
                "gcm": self.gcms[gcm],
                "year": year,
                "categories": ["current"],
            }
        LOG.info('Import %s', item['title'])
        return item


@provider(ISectionBlueprint)
@implementer(ISection)
class ClimondLayers(WorldClimLayer):

    # map emscs from file name to vorabulary id
    emscs = {
        'SRES-A2': 'SRESA2',
        'SRES-A1B': 'SRESA1B'
    }
    # map gcms from file name to vocab id
    gcms = {
        'MIROC-H': 'ccsr-miroc32hi',
        'CSIRO-Mk3.0': 'csiro-mk30',
    }

    def __init__(self, transmogrifier, name, options, previous):
        self.transmogrifier = transmogrifier
        self.context = transmogrifier.context
        self.name = name
        self.options = options
        self.previous = previous

        # get filters from configuration
        self.enabled = options.get('enabled', "").lower() in (
            "true", "1", "on", "yes")

    def __iter__(self):
        # exhaust previous
        for item in self.previous:
            yield item

        if not self.enabled:
            return

        # tell our event stats event handler that we collect stats later
        IAnnotations(self.context.REQUEST)['org.bccvl.site.stats.delay'] = True

        # Current climate datasets
        yield self._createCurrentItem()

        # Future climate datasets
        for emsc in self.emscs.keys():
            for gcm in self.gcms.keys():
                for year in [2030, 2050, 2070, 2090, 2100]:
                    yield self._createItem(emsc, gcm, year)

    def _createItem(self, emsc, gcm, year):
        res = "10m"
        filename = 'CLIMOND_{emsc}_{gcm}_{year}.zip'.format(
            emsc=emsc, gcm=gcm, year=year)
        item = {
            '_path': 'datasets/climate/climond/{}/{}'.format(res, filename),
            '_owner': (1, 'admin'),
            "_type": "org.bccvl.content.remotedataset",
            "title": u'CliMond (global), future climate ({year}), ({emsc}) based on {gcm}, 10 arcmin (~20 km)'.format(
                emsc=emsc_title(self.context, self.emscs[emsc]), gcm=gcm.upper(), year=year),
            "description": u"CLIMOND Bioclimate Map Time-Series, 1975 - 2100.  A set of 35 bioclimatic variables (30-year average) with 10 arcminute resolution, calculated according to the WorldClim method.",
            "remoteUrl": '{0}/climond/{1}'.format(SWIFTROOT, filename),
            "format": "application/zip",
            "creators": 'BCCVL',
            "dataSource": "ingest",
            "_transitions": "publish",
            "subject": [TERRESTRIAL_DATASET_TAG],
            "bccvlmetadata": {
                "genre": "DataGenreFC",
                "resolution": 'Resolution{}'.format(res),
                "emsc": self.emscs[emsc],
                "gcm": self.gcms[gcm],
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
            "title": u'CliMond (global), current climate (1975), 10 arcmin (~20 km)',
            "description": u"CLIMOND Bioclimate Map Time-Series, 1975 - 2100.  A set of 35 bioclimatic variables (30-year average) with 10 arcminute resolution, calculated according to the WorldClim method.",
            "remoteUrl": '{0}/climond/{1}'.format(SWIFTROOT, filename),
            "format": "application/zip",
            "creators": 'BCCVL',
            "dataSource": "ingest",
            "_transitions": "publish",
            "subject": [TERRESTRIAL_DATASET_TAG, SUMMARY_DATASET_TAG],
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

    # map emscs from file name to vorabulary id
    emscs = {
        'SRES-A2': 'SRESA2',
    }
    # map gcms from file name to vocab id
    gcms = {
        'CCCMA3.1': 'cccma-cgcm31',
        'CSIRO-MK3.0': 'csiro-mk30',
        'ECHAM5': 'mpi-echam5',
        'MIROC3.2': 'ccsr-miroc32med',
    }
    # map rcms from file name to vocab id
    rcms = ['R1', 'R2', 'R3']

    # NaRCLIM current datasets
    current_datasets = [
        #('NaRCLIM_baseline_Aus_Extent.zip', '36s', 2000),
        ('NaRCLIM_baseline_NaR_Extent.zip', '36s', 2000),
        # ('NaRCLIM_baseline.zip', '9s', 2000)
    ]

    def __init__(self, transmogrifier, name, options, previous):
        self.transmogrifier = transmogrifier
        self.context = transmogrifier.context
        self.name = name
        self.options = options
        self.previous = previous

        # get filters from configuration
        self.enabled = options.get('enabled', "").lower() in (
            "true", "1", "on", "yes")

    def __iter__(self):
        # exhaust previous
        for item in self.previous:
            yield item

        if not self.enabled:
            return

        # tell our event stats event handler that we collect stats later
        IAnnotations(self.context.REQUEST)['org.bccvl.site.stats.delay'] = True

        # Future climate datasets
        for gcm in self.gcms.keys():
            for rcm in self.rcms:
                for year in [2030, 2070]:
                    for res in ['36s']:  # ['36s', '9s']:
                        yield self._createItem(gcm, rcm, res, year)

        # Current climate datasets
        for filename, res, year in self.current_datasets:
            yield self._createCurrentItem(filename, res, year)

    def _createItem(self, gcm, rcm, res, year):
        if res == '36s':
            resolution = '36 arcsec (~1 km)'
        else:
            resolution = '9 arcsec (~250 m)'
        filename = 'NaRCLIM_{gcm}_{rcm}_{year}.zip'.format(
            gcm=gcm, rcm=rcm, year=year)
        item = {
            '_path': 'datasets/climate/narclim/{}/{}'.format(res, filename),
            '_owner': (1, 'admin'),
            "_type": "org.bccvl.content.remotedataset",
            "title": u'South-East Australia Future Climate, ({year}), (SRES-A2) based on {gcm}-{rcm}, {resolution}'.format(gcm=gcm.upper(), rcm=rcm.upper(), resolution=resolution, year=year),
            "description": u"South-East Australia Bioclimate Maps: year {year}. A set of 35 bioclimatic variables (20-year average) for NSW, VIC & ACT with {resolution} resolution, calculated according to the WorldClim method.".format(year=year, resolution=resolution),
            "remoteUrl": '{0}/narclim/{1}/{2}'.format(SWIFTROOT, res, filename),
            "format": "application/zip",
            "creators": 'BCCVL',
            "dataSource": "ingest",
            "_transitions": "publish",
            "subject": [TERRESTRIAL_DATASET_TAG],
            "bccvlmetadata": {
                "genre": "DataGenreFC",
                # This shall match to the resolution vacab in registry
                "resolution": 'Resolution{}'.format(res),
                "emsc": self.emscs['SRES-A2'],
                "gcm": self.gcms[gcm],
                "rcm": rcm,
                "year": year,
                "categories": ["future"],
            },
        }

        LOG.info('Import %s', item['title'])
        return item

    def _createCurrentItem(self, filename, res, year):
        if res == '36s':
            resolution = '36 arcsec (~1 km)'
        else:
            resolution = '9 arcsec (~250 m)'
        item = {
            '_path': 'datasets/climate/narclim/{}/{}'.format(res, filename),
            '_owner': (1, 'admin'),
            "_type": "org.bccvl.content.remotedataset",
            "title": u'South-East Australia Current Climate, ({year}), {resolution}'.format(resolution=resolution, year=year),
            "description": u"South-East Australia Bioclimate Maps: year {year}. A set of 35 bioclimatic variables (20-year average) for NSW, VIC & ACT with {resolution} resolution, calculated according to the WorldClim method.".format(year=year, resolution=resolution),
            "remoteUrl": '{0}/narclim/{1}/{2}'.format(SWIFTROOT, res, filename),
            "format": "application/zip",
            "creators": 'BCCVL',
            "dataSource": "ingest",
            "_transitions": "publish",
            "subject": [TERRESTRIAL_DATASET_TAG],
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
class ANUClimLayers(WorldClimLayer):
    # ANUClim monthly datasets
    MONTH_LIST = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']

    def __init__(self, transmogrifier, name, options, previous):
        self.transmogrifier = transmogrifier
        self.context = transmogrifier.context
        self.name = name
        self.options = options
        self.previous = previous

        # get filters from configuration
        self.enabled = options.get('enabled', "").lower() in (
            "true", "1", "on", "yes")

    def __iter__(self):
        # exhaust previous
        for item in self.previous:
            yield item

        if not self.enabled:
            return

        # tell our event stats event handler that we collect stats later
        IAnnotations(self.context.REQUEST)['org.bccvl.site.stats.delay'] = True

        # Current climate datasets
        res = "30s"
        resolution = "1km"
        year = "1976-2005"
        for month in range(12):
            filename = 'anuclim_{}_{}.zip'.format(resolution, self.MONTH_LIST[month][:3])
            yield self._createCurrentItem(filename, res, self.MONTH_LIST[month], year)

    def _createCurrentItem(self, filename, res, month, year):
        resolution = ''
        if res == '30s':
            resolution = '30 arcsec (~1 km)'
        item = {
            '_path': 'datasets/climate/anuclim/{}/{}'.format(res, filename),
            '_owner': (1, 'admin'),
            "_type": "org.bccvl.content.remotedataset",
            "title": u'ANUClim (Australia), Current Climate {month}, ({year}), {resolution}'.format(resolution=resolution, month=month, year=year),
            "description": u"Monthly climate data for the Australian continent between {year}, generated using ANUClimate 1.0. This dataset includes 5 variables: monthly mean precipitation, mean daily minimum and maximum temperature of the month, mean daily vapour pressure of the month and monthly total class A pan evaporation. The monthly anomalies were interpolated by trivariate thin plate smoothing spline functions of longitude, latitude and vertically exaggerated elevation using ANUSPLIN Version 4.5. Monthly data values were calculated from Bureau of Meteorology daily data at stations where there were no missing observations and any accumulated records were wholly within the month.".format(year=year),
            "remoteUrl": '{0}/anuclim/{1}'.format(SWIFTROOT, filename),
            "format": "application/zip",
            "creators": 'BCCVL',
            "dataSource": "ingest",
            "_transitions": "publish",
            "subject": [TERRESTRIAL_DATASET_TAG],
            "bccvlmetadata": {
                "genre": "DataGenreCC",
                "resolution": 'Resolution{}'.format(res),
                "categories": ["current"],
            },
            "subject": [MONTHLY_DATASET_TAG],
        }

        LOG.info('Import %s', item['title'])
        return item


@provider(ISectionBlueprint)
@implementer(ISection)
class GeofabricLayers(WorldClimLayer):

    dataset_info = {
        'catchment': {'climate': ('current', u'Aggregated climate data for the Australian continent between 1921-1995, generated using ANUCLIM version 6.1, for catchments derived from the national 9 arcsec DEM and flow direction grid version 3. Catchments consist of all grid cells upstream of the center of the stream segment pour-point cell. The bioclim layers are the average values of the corresponding bioclim parameters calculated for all.'),
                      'vegetation': ('vegetation', u'Natural (pre-1750) and extant (present day) vegetation cover for catchments across the Australian continent based on the NVIS Major Vegetation sub-groups version 3.1. Catchments consist of all grid cells upstream of the center of the stream segment pour-point cell.'),
                      'substrate': ('substrate', u'Substrate data with soil hydrological characteristics and lithological composition for catchments across the Australian continent based on the surface geology of Australia 1:1M. Catchments consist of all grid cells upstream of the center of the stream segment pour-point cell.'),
                      'terrain': ('topography', u'Terrain data for catchments across the Australian continent based on the 9" DEM of Australia version 3 (2008). Catchments consist of all grid cells upstream of the center of the stream segment pour-point cell.'),
                      'landuse': ('landuse', u'Land use data reflecting the proportion of 13 different land use activities (based on the tertiary land use classification by M. Stewardson, University of Melbourne, 2010) for catchments across the Australian continent based on the Catchment-scale land use mapping for Australia (Bureau of Rural Sciences, 2009). Catchments consist of all grid cells upstream of the center of the stream segment pour-point cell.'),
                      'population': ('landuse', u'Population data for catchments across the Australian continent based on the population density in 2006 (Australian Bureau of Statistics). Catchments consist of all grid cells upstream of the center of the stream segment pour-point cell.'),
                      'npp': ('productivity', u'Average of annual and monthly mean net primary productivity (NPP) for catchments across the Australian continent based on Raupach et al. (2001). NPP is equal to plant photosynthesis less plant respiration, and reflects the carbon or biomass yield of the landscape, available for use by animals and humans. Catchments consist of all grid cells upstream of the center of the stream segment pour-point cell.'),
                      'rdi': ('landuse', u'Indicators of pressure on stream ecosystems due to human activities derived using the method of <a href=\"http://www.sciencedirect.com/science/article/pii/S0169204602000488\" target=\"_blank\">Stein et al. (2002)</a>. The method couples geographical data, recording the extent and intensity of human activities known to impact on river condition, with a Digital Elevation Model (DEM) used for drainage analysis. The indices rank streams along a continuum from near-pristine to severely disturbed.')},
        'stream':    {'climate': ('current', u'Aggregated climate data for the Australian continent between 1921-1995, generated using ANUCLIM version 6.1, for stream segments derived from the national 9 arcsec DEM and flow direction grid version 3. Stream segments refer to all grid cells comprising the stream segment and associated valley bottom. The bioclim layers are the average values of the corresponding bioclim parameters calculated for all.'),
                      'vegetation': ('vegetation', u'Natural (pre-1750) and extant (present day) vegetation cover for stream segments across the Australian continent based on the NVIS Major Vegetation sub-groups version 3.1. Stream segments refer to all grid cells comprising the stream segment and associated valley bottom.'),
                      'substrate': ('substrate', u'Substrate data with soil hydrological characteristics and lithological composition for stream segments across the Australian continent based on the surface geology of Australia 1:1M. Stream segments refer to all grid cells comprising the stream segment and associated valley bottom.'),
                      'terrain': ('topography', u'Terrain data for stream segments across the Australian continent based on the 9" DEM of Australia version 3 (2008). Stream segments refer to all grid cells comprising the stream segment and associated valley bottom.'),
                      'landuse': ('landuse', u'Land use data reflecting the proportion of 13 different land use activities (based on the tertiary land use classification by M. Stewardson, University of Melbourne, 2010) for stream segments across the Australian continent based on the Catchment-scale land use mapping for Australia (Bureau of Rural Sciences, 2009). Stream segments refer to all grid cells comprising the stream segment and associated valley bottom.'),
                      'population': ('landuse', u'Population data for stream segments across the Australian continent based on the population density in 2006 (Australian Bureau of Statistics). Stream segments refer to all grid cells comprising the stream segment and associated valley bottom.'),
                      'network': ('hydrology', u'Network parameters of habitat availability for stream segments and associated vallye bottoms and sub-catchments across the Australian continent based on AusHydro version 1.1.6 (Bureau of Meteorology, 2010).'),
                      'connectivity': ('hydrology', u'Connectivity parameters that indicate presence of major in-stream barriers including dams and waterfalls for stream segments and associated vallye bottoms and sub-catchments across the Australian continent based on AusHydro version 1.1.6 (Bureau of Meteorology, 2010).')}
    }

    external_description = [
            u'The layers in this dataset were developed by the Australian National University (ANU) in 2011 and updated in 2012. BCCVL has integrated version 1.1.5 (2012) of the database.',
            u'Publication: <a href=\"http://www.hydrol-earth-syst-sci.net/18/1917/2014/hess-18-1917-2014.pdf\" target=\"_blank\">http://www.hydrol-earth-syst-sci.net/18/1917/2014/hess-18-1917-2014.pdf</a>',
            u'Data source: <a href=\"https://data.gov.au/dataset/national-environmental-stream-attributes-v1-1-5\" target=\"_blank\">https://data.gov.au/dataset/national-environmental-stream-attributes-v1-1-5</a>'
    ]

    rdi_external_description = [
            u'The layers in this dataset were developed by the Australian National University (ANU) in 2011 and updated in 2012. BCCVL has integrated version 1.1.5 (2012) of the database. The source data for these layers are described in Stein et al. (1998; The identification of wild rivers) updated with catchment-scale land use mapping for Australia (Bureau of Rural Sciences, 2009), Geodata Topo 250K series 2 (Geoscience Australia, 2003), Integrated Vegetation Cover (Bureau of Rural Sciences, 2003).',
            u'Publication: <a href=\"http://www.hydrol-earth-syst-sci.net/18/1917/2014/hess-18-1917-2014.pdf\" target=\"_blank\">http://www.hydrol-earth-syst-sci.net/18/1917/2014/hess-18-1917-2014.pdf</a>',
            u'Data source: <a href=\"https://data.gov.au/dataset/national-environmental-stream-attributes-v1-1-5\" target=\"_blank\">https://data.gov.au/dataset/national-environmental-stream-attributes-v1-1-5</a>'
    ]

    # Geofabric datasets
    def __init__(self, transmogrifier, name, options, previous):
        self.transmogrifier = transmogrifier
        self.context = transmogrifier.context
        self.name = name
        self.options = options
        self.previous = previous

        # get filters from configuration
        self.btype = set(x.strip()
                        for x in options.get('btype', "").split(',') if x)
        self.dstype = set(x.strip()
                       for x in options.get('dstype', "").split(',') if x)

        self.enabled = options.get('enabled', "").lower() in (
            "true", "1", "on", "yes")

    def __iter__(self):
        # exhaust previous

        for item in self.previous:
            yield item

        if not self.enabled:
            return

        # tell our event stats event handler that we collect stats later
        IAnnotations(self.context.REQUEST)['org.bccvl.site.stats.delay'] = True

        boundary_types = ['catchment', 'stream']
        # attribute datasets
        for boundtype in boundary_types:
            if self.btype and boundtype not in self.btype:
                continue

            for datasettype in self.dataset_info[boundtype]:
                if self.dstype and datasettype not in self.dstype:
                    continue
                yield self._createAttributeItem(boundtype, datasettype)

    def _createAttributeItem(self, boundtype, dstype):
        # dataset filename
        filename = 'geofabric_{}_{}.zip'.format(boundtype, dstype)
        if dstype == 'climate':
            attrname = 'Current Climate (1921-1995)'
        elif dstype == 'npp':
            attrname = 'Net Primary Productivity'
        elif dstype == 'landuse':
            attrname = 'Land Use'
        elif dstype == 'rdi':
            attrname = 'River Disturbance'
        else:
            attrname = dstype.title()

        if dstype == 'rdi':
            title = u'Freshwater Data (Australia), {attrname}, 9 arcsec (~250m)'.format(attrname=attrname)
            full_description = u'<br>'.join(self.rdi_external_description)
        else:
            title = u'Freshwater {btype} Data (Australia), {attrname}, 9 arcsec (~250m)'.format(btype=boundtype.title(), attrname=attrname)
            full_description = u'<br>'.join(self.external_description)


        item = {
            '_path': 'datasets/{0}/{1}'.format(
                'climate/geofabric/9s' if dstype == 'climate' else 'environmental/geofabric',
                filename),
            '_owner': (1, 'admin'),
            "_type": "org.bccvl.content.remotedataset",
            "title": title,
            "description": self.dataset_info[boundtype][dstype][1],
            "external_description": full_description,
            "remoteUrl": '{0}/geofabric/{1}'.format(SWIFTROOT, filename),
            "format": "application/zip",
            "creators": 'BCCVL',
            "dataSource": "ingest",
            "_transitions": "publish",
            "subject": [FRESHWATER_DATASET_TAG, SUMMARY_DATASET_TAG],
            "bccvlmetadata": {
                "genre": "DataGenreCC" if dstype == "climate" else "DataGenreE",
                "resolution": 'Resolution9s',
                "categories": [ self.dataset_info[boundtype][dstype][0] ],
            },
        }

        LOG.info('Import %s', item['title'])
        return item

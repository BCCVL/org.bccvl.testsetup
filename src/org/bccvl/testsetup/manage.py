import argparse
import logging
import sys

from AccessControl.SecurityManagement import newSecurityManager
from AccessControl.SecurityManager import setSecurityPolicy
from AccessControl.SpecialUsers import system
from Products.CMFCore.tests.base.security import PermissiveSecurityPolicy, OmnipotentUser
from Testing.makerequest import makerequest
import transaction
from zope.component.hooks import setSite


LOG = logging.getLogger(__name__)


def spoofRequest(app):
    """
    Make REQUEST variable to be available on the Zope application server.

    This allows acquisition to work properly
    """
    _policy = PermissiveSecurityPolicy()
    _oldpolicy = setSecurityPolicy(_policy)
    newSecurityManager(None, OmnipotentUser().__of__(app.acl_users))
    return makerequest(app)


def create_site(app, params):
    site_id = params['site_id']
    oids = app.objectIds()
    if site_id in oids:
        if params['site_replace']:
            LOG.warning("Removing existing Site with id: %s", site_id)
             # Delete the site, ignoring events
            app._delObject(site_id, suppress_events=True)
            transaction.commit()
            oids = app.objectIds()
        else:
            LOG.warning("Site with id %s aready exists. Nothing to do.", site_id)
            return app[site_id], False
    if site_id not in oids:
        LOG.info("Adding Site: %s", site_id)
        # Create Site via form
        app.REQUEST.form.update({
            'form.submitted': True,
            'site_id': site_id,
            'title': unicode(params['title']),
            'extension_ids': ['plonetheme.sunburst:default'],
            'setup_content': False,
            'default_language': 'en'})
        form = app.restrictedTraverse('@@plone-addsite')
        # Skip the template rendering
        form.index = lambda: None
        form()
        transaction.commit()
        LOG.info("Added Site: %s", site_id)
    return app[site_id], True


def parse_args(args):
    parser = argparse.ArgumentParser(description='Create / Upgrate Site.')
    parser.add_argument('--upgrade', action='store_true', default=False)
    parser.add_argument('--id', default='bccvl', dest='site_id')
    parser.add_argument('--title', default='BCCVL')
    parser.add_argument('--replace', action='store_true', dest='site_replace', default=False)
    parser.add_argument('--lastupgrade', action='store_true', default=False)
    pargs = parser.parse_args(args)
    return vars(pargs)


def main(app, params):
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)

    logging.getLogger('ZODB.Connection').setLevel(logging.WARN)
    # logging.getLogger('').setLevel(logging.WARN)
    # logging.getLogger('').setLevel(logging.WARN)

    app = spoofRequest(app)
    newSecurityManager(None, system)
    site, created = create_site(app, params)

    # setup component architecture
    setSite(site)

    # Install our addons
    qi = site.portal_quickinstaller
    not_installed = [x['id'] for x in qi.listInstallableProducts(skipInstalled=1)]
    for product in ('org.bccvl.site', 'org.bccvl.theme'):
        if product in not_installed:
            qi.installProduct(product)
            transaction.commit()

    # upgrade requested?
    if not (params['upgrade'] or params['lastupgrade']):
        return

    site.setupCurrentSkin(app.REQUEST)
    # get portal_setup to finde available upgrades
    ps = site.portal_setup
    # get baseline profile and check for upgrades
    _, profile = ps.getBaselineContextID().split('-', 1)
    upgrades = ps.listUpgrades(profile)
    if upgrades:
        LOG.info('Upgrading baseline profile %s', profile)
        app.REQUEST.form.update({
            'upgrades': [],
            'profile_id': profile
        })
        ps.manage_doUpgrades()
        transaction.commit()
    # find bccvl upgrade steps
    profile = 'org.bccvl.site:default'
    if params['lastupgrade']:
        # filter out last upgrade step and wrap it back into a list of lists
        upgrades = [ps.listUpgrades(profile, show_old=True)[-1]]
    else:
        upgrades = ps.listUpgrades(profile)
    if upgrades:
        LOG.info('Running upgrade profile for %s', profile)
        for steps in upgrades:
            if isinstance(steps, list):
                # we have multiple steps here
                app.REQUEST.form.update({
                    'upgrades': [x['id'] for x in steps],
                    'profile_id': profile
                })
            else:
                app.REQUEST.form.update({
                    'upgrades': steps['id'],
                    'profile_id': profile
                })
        ps.manage_doUpgrades()
        transaction.commit()


def zopectl(app, args):
    """ zopectl entry point
    app ... the Zope root Application
    args ... list of command line args passed (very similar to sys.argv)
             args[0] ... name of script but is always '-c'
             args[1] ... name of entry point
             args[2:] ... all additional commandline args
    """
    # get rid of '-c'
    if args[0] == '-c':
        args.pop(0)
    # now args looks pretty much like sys.argv
    params = parse_args(args[1:])
    # ok let's do some import'
    main(app, params)


if 'app' in locals():
    # we have been started via ./bin/instance run main.py
    # but ideally should be run via ./bin/instance testsetup
    main(app, parse_args(sys.argv[4:]))

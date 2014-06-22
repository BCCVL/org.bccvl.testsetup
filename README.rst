org.bccvl.testsetup
===================

Create Test setup ond configuration for BCCVL site

After installing this package Zope instances offer a new command,
which can be used to install a subset (or all) of the defined future
climate layers

Usage:
======

Enable python27::

  scl enable python27 bash

Install::

  # ./bin/instance-debug testsetup [--test|--all]

no params ... install datasets enbedded in this package
--test ... install a few remote datasets for testing
--all  ... install all known datasets

org.bccvl.testsetup
===================

Create Test setup ond configuration for BCCVL site

After installing this package Zope instances offer a new command,
which can be used to install a subset (or all) of the defined future
climate layers

Usage:
======

  # ./bin/instance-debug testsetup --gcm RCP3PD --emsc cccma-cgcm31 --year 2015

Each command line parameter can be specified 0 or more times and the
command will only install matching future climate layers.

E.g. the following command will only install datasets with GCM RCP3PD
or RCP6 and emissionscenario cccma-cgcm31 for all available years

  # ./bin/instance-debug testsetup --gcm RCP3PD --gcm RCP6 --emsc cccma-cgcm31

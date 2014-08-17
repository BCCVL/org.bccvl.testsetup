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
           same as: --nsgsource, --vastsource --a5ksource, --emsc=RCP3PD --gcm=ccma-cgcm31 --year=2015,2025

--all  ... install all known datasets

--a5ksource ... enable 5km future climate datasets
  --emsc ... comma separated list of emission scenarios:
             RCP3PD, RCP45, RCP6, RCP85,
             SRESA1B, SRESA1FI, SRESA2, SRESB1, SRESB2
  --gcm ... comma separated list of circulation models:
            cccma-cgcm31, ccsr-miroc32hi, ccsr-micro32med,
            cnrm-cm3, csiro-mk30, gfdl-cm20, gfdl-cm21,
            giss-modeleh, giss-modeler, iap-fgoals10g, inm-cm30,
            ipsl-cm4, mpi-echam5, mri-cgcm232a, ncar-ccsm30,
            ncar-pcm1, ukmo-hadcm3, ukmo-hadgem1
  --years ... comma separated list of years
              2015, 2025, 2035, 2045, 2055, 2065, 207', 2085

--nsgsource ... enable national soil grid dataset
--vastsource ... enable vast dataset
--mrrtfsource ... enable multi res. rich top flatness dataset
--mrvbfsource ... enable multi res. valley ?? flatness dataset

import unittest,sys,os,traceback
from zope.testing import doctest, testrunner


def test_suite():

  #  suite = unittest.makeSuite()
    return unittest.TestSuite((
	    doctest.DocTestSuite('Rambler.ciConfigService'),
	    doctest.DocTestSuite('Rambler.Application'),
#	    doctest.DocTestSuite('Rambler.ErrorFactory')

	    #doctest.DocTestSuite('switchboard'),
	    ))

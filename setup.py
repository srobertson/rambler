from setuptools import setup

setup(
    name='rambler',
    version = '2.0',
    description='Framework for building Async applications',
    author='Scott Robertson',
    author_email='srobertson@codeit.com',
    #package_dir = {'': 'src'},
    packages = ['Rambler'],
    requires = ['zope.interface']
    #test_suite = 'your.module.tests',
)
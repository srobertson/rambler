from setuptools import setup

setup(
    name='rambler',
    version = '2.0',
    description='Framework for building Async applications',
    author='Scott Robertson',
    author_email='srobertson@codeit.com',
    #package_dir = {'': 'src'},
    packages = ['Rambler','Rambler.controllers'],
    install_requires = ['zope.interface','python-dateutil==1.5','yaml','pyparsing','pyMoney'],
    
    entry_points={
              'console_scripts': [
                  'ramblerapp = Rambler.Application:main',
                  ]
                }

)

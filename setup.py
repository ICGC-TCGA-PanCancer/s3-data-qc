from setuptools import setup

setup(
    name='s3objectqc',
    version='0.0.1',
    description='Perform various QC on S3 objects',
    packages=['s3objectqc'],
    install_requires=[
      'pyyaml',
      'xmltodict',
      'requests'
    ],
    entry_points='''
    [console_scripts]
    s3objectqc=s3objectqc.__main__:main
    '''
)

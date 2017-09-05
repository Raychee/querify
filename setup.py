from distutils.core import setup

from querify import VERSION


setup(
    name='querify',
    packages=['querify'],
    version=VERSION,
    description='InfluxQL / MySQL / Mongo query generator from json-represented filters (mongodb-like query objects)',
    author='Raychee Zhang',
    author_email='raychee.zhang@gmail.com',
    url='https://github.com/Raychee/querify',
    download_url='https://github.com/Raychee/querify/tarball/' + VERSION,
    keywords=['json', 'mongo', 'influx', 'mysql'],
    install_requires=[
        'qutils>=0.3.0'
    ]
)

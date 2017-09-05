from setuptools import setup, find_packages


VERSION = ''

with open('querify/__init__.py') as f:
    exec(f.readline())

setup(
    name='querify',
    version=VERSION,
    description='InfluxQL / MySQL / Mongo query generator from json-represented filters (mongodb-like query objects)',
    author='Raychee Zhang',
    author_email='raychee.zhang@gmail.com',
    license='MIT',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Framework :: Pytest',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3 :: Only',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Utilities'
    ],
    keywords=['json', 'mongo', 'influx', 'mysql', 'query'],
    url='https://github.com/Raychee/querify',
    download_url='https://github.com/Raychee/querify/tarball/' + VERSION,
    packages=find_packages(exclude=['*.test']),
    setup_requires=['pytest-runner'],
    tests_require=['pytest'],
    install_requires=[
        'qutils>=0.3.0'
    ],
    python_requires='>=3.5',
)

from setuptools import setup, find_packages

__version__ = '1.0.0'


install_requires = {
    'requests'
    'boto3'
    'commandr'
}

extra_requires = {
    'sql': ['SQLAlchemy'],
    'redis': ['redis']
}

tests_require = {}

setup(
    name='monday',
    version=__version__,
    license='BSD',
    packages=find_packages(),
    zip_safe=False,
    description='Monday',
    long_description=__doc__,
    author='sekhou',
    author_email='sekhou64@gmail.com',
    install_requires=install_requires,
    extra_requires=extra_requires,
    tests_require=tests_require,
    platforms='any',
    entry_points={}
)

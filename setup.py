import setuptools
import codecs
import os
import re

here = os.path.abspath(os.path.dirname(__file__))


def read(*parts):
    # intentionally *not* adding an encoding option to open, See:
    #   https://github.com/pypa/virtualenv/issues/201#issuecomment-3145690
    with codecs.open(os.path.join(here, *parts), 'r') as fp:
        return fp.read()


def get_version():
    version_file = read('src', 'migrator', '__init__.py')
    version_match = re.search(r'^__version__ = [\'"]([^\'"]*)[\'"]',
                              version_file, re.MULTILINE)
    if version_match:
        return version_match.group(1)
    raise RuntimeError('Unable to find version string.')


setuptools.setup(
    name="dynamodb-migrator",
    version=get_version(),
    author="Bert Blommers",
    author_email="info@bertblommers.nl",
    description="Library that helps you create and migrate DynamoDB databases",
    long_description=read('README.md'),
    long_description_content_type="text/markdown",
    url="https://github.com/bblommers/dynamodb-migrator",
    packages = setuptools.find_namespace_packages(where='src'),
    package_dir = {'': 'src'},
    classifiers=[
        "Programming Language :: Python :: 3",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries",
        "Topic :: Utilities"
    ],
    python_requires='>=3.6',
)

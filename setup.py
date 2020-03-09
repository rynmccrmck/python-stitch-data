from setuptools import setup, find_packages
from os import path

this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='python-stitch-data',
    version='0.1.0',
    description='Python wrapper for Stitch Data API',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/rynmccrmck/python-stitch-data',
    download_url='https://github.com/rynmccrmck/python-stitch-data/archive/v0.1.0.tar.gz',
    keywords=['stitch', 'api', 'wrapper'],
    packages=find_packages(),
    install_requires=[
         'click',
         'python-dotenv',
         'requests-toolbelt'
     ],
    entry_points={
        'console_scripts': [
            'stitchapi = stitch_api.cli:main',
        ],
    },
)

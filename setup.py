from setuptools import setup, find_packages
from os import path

this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    # other arguments here...
    name='python-stitch-data',
    version='1.0.1',
    description='Python wrapper for Stitch Data API',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/rynmccrmck/python-stitch-data',
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

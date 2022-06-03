from setuptools import setup

setup(
    name='runops',
    version='0.0.1',
    author='Alison Alonso',
    license='Postgresql',
    install_requires=[
        'requests'
    ],
    packages=['runops', 'runops.api', 'runops.fdw']
)

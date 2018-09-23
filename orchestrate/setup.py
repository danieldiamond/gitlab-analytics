from setuptools import setup, find_packages
setup(
    # this is too generic, maybe change?
    name="orchestrate",
    version="0.1",
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'orchestrate = orchestrate.cli:main'
        ]
    },
    # this needs to be pared down to the actual deps
    install_requires=[
        'aiodns==1.1.1',
        'aiohttp==3.3.2',
        'apscheduler==3.5.3',
        'cchardet==2.1.1',
        'fire==0.1.3',
        'pytest==3.5.1',
        'pytest-asyncio==0.9.0',
        'pytz==2017.2',
        'pyyaml==3.13',
        'requests==2.18.4',
        'sqlalchemy==1.2.12',
        'psycopg2-binary==2.7.5',
    ],
)

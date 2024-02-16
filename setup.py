from setuptools import setup

setup(
    name='py-rq',
    version='4.0.0',
    packages=['pyrq'],
    url='https://github.com/heureka/py-rq',
    license='MIT',
    author='Heureka.cz',
    author_email='podpora@heureka.cz',
    description='Redis queue for Python',
    install_requires=[
        "redis>=3.0.0,<4"
    ]
)

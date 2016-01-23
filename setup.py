from setuptools import setup

setup(
    name='py-rq',
    version='1.0.0',
    packages=['pyrq'],
    url='ssh://git@github.com:heureka/py-rq.git',
    license='MIT',
    author='Heureka.cz',
    author_email='podpora@heureka.cz',
    description='Redis queue for Python',
    install_requires=[
        "redis>2.0.0"
    ]
)

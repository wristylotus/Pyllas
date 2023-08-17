import pyllas
from setuptools import setup, find_packages


def readme():
    with open('README.md', 'r') as f:
        return f.read()


setup(
    name='pyllas',
    version=pyllas.__version__,
    author='wristylotus',
    author_email='wristylotus@gmail.com',
    description='Client for AWS Athena',
    long_description=readme(),
    long_description_content_type='text/markdown',
    url='https://github.com/wristylotus/Pyllas',
    packages=find_packages(),
    install_requires=[
        'boto3>=1.26.42',
        'pandas>=1.5.0',
        'pyarrow>=8.0.0',
    ],
    classifiers=[
        'Programming Language :: Python :: 3.7',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent'
    ],
    keywords='pyllas aws athena sql jupyter',
    project_urls={
        'Documentation': 'https://github.com/wristylotus/Pyllas/blob/main/README.md'
    },
    python_requires='>=3.7'
)

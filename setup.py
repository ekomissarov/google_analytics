import setuptools
# https://packaging.python.org/tutorials/packaging-projects/

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pysea-google-analytics", # Replace with your own username
    version="0.0.11",
    author="Eugene Komissarov",
    author_email="ekom@cian.ru",
    description="Google Analytics base",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="git@bitbucket.org:cianmedia/google_analytics.git",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: Linux",
    ],
    python_requires='>=3.7',
    install_requires=[
        'pysea-common-constants',
        'cachetools>=4.1.1',
        'certifi>=2020.6.20',
        'chardet>=3.0.4',
        'google-api-core>=1.21.0',
        'google-api-python-client>=1.9.3',
        'google-auth>=1.19.0',
        'google-auth-httplib2>=0.0.4',
        'googleapis-common-protos>=1.52.0',
        'httplib2>=0.18.1',
        'idna>=2.10',
        'oauth2client>=4.1.3',
        'protobuf>=3.12.2',
        'pyasn1>=0.4.8',
        'pyasn1-modules>=0.2.8',
        'pytz>=2020.1',
        'requests>=2.24.0',
        'rsa>=4.6',
        'six>=1.15.0',
        'uritemplate>=3.0.1',
        'urllib3>=1.25.9',
    ]
)
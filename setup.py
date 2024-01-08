# type: ignore
import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="memproxy",
    version="0.3.0rc21",
    license="MIT",
    keywords=["Redis", "key-value store", "caching"],

    author="quangtung97",
    author_email="quangtung29121997@gmail.com",

    description="A library for strong consistent caching",
    long_description=long_description,
    long_description_content_type="text/markdown",

    url="https://github.com/QuangTung97/memproxy-py",
    project_urls={
        "Changes": "https://github.com/QuangTung97/memproxy-py/releases",
        "Code": "https://github.com/QuangTung97/memproxy-py",
        "Issue Tracker": "https://github.com/QuangTung97/memproxy-py/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    package_data={
        'memproxy': ['py.typed'],
        'memproxy.proxy': ['py.typed'],
    },
    packages=['memproxy', 'memproxy.proxy'],
    python_requires=">=3.7",
    setup_requires=['wheel'],
    install_requires=[
        'redis>=3.2.0',
    ],
)

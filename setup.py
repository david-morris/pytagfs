import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()


setuptools.setup(
    name='pytagfs',
    version='0.0.2',
    scripts=['pytagfs'],
    author="David Morris",
    author_email="davidrsmorris@gmail.com",
    description="A FUSE filesystem which manages files via tags instead of containing them in folders.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/david-morris/pytagfs",
    install_requires=['fusepy'],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Development Status :: 2 - Pre-Alpha",
        "Environment :: No Input/Output (Daemon)",
        "Intended Audience :: End Users/Desktop",
        "License :: OSI Approved :: GNU Affero General Public License v3",
        "Operating System :: POSIX",
        "Topic :: System :: Filesystems",
    ],
)

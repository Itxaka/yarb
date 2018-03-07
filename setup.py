from setuptools import setup

setup(
    name="yarb",
    version="1.0.0",
    description="Yet Another Rabbit Balancer",
    author="Itxaka Serrano Garcia",
    author_email="igarcia@suse.com",
    url="https://github.com/Itxaka/yarb",
    license="GPL2.0",
    packages=["yarb"],
    entry_points={
        "console_scripts": [
            "yarb = yarb.yarb:main",
        ],
    },
    install_requires=["requests"],
    tests_require=["requests", "mock"],
    package_data={"": ["LICENSE", "README.md"]},
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Environment :: Console"
    ],
)

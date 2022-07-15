import pathlib
import setuptools


LOCATION = pathlib.Path(__file__).parent.resolve()

readme_file = LOCATION / "README.md"
long_description = readme_file.open(encoding="utf8").read()

setuptools.setup(
    name="dff_node_stats",
    version="0.1.2",
    scripts=[],
    author="Denis Kuznetsov",
    author_email="kuznetsov.den.p@gmail.com",
    description="Statistics collection extension for Dialog Flow Framework "
    "(https://github.com/deepmipt/dialog_flow_framework).",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/kudep/dff-node-stats",
    packages=setuptools.find_packages(where="."),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        "pandas>=1.3.1",
        "df_engine>=0.10.0",
        "tqdm>=4.62.3",
        "pydantic>=1.8.2",
    ],
)

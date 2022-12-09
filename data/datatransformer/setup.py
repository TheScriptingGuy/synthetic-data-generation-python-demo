import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="datatransformer",
    # Do not change! This is hard-coded on purpose,
    # so that we don't have to account for versions in infrastructure/databricks/Config
    version="0.0.1",
    author="Wessel van der Linden, Mark Streutker",
    author_email="wessel.vanderlinden@infosupport.com",
    description="Transform Data to be used for Synthetic Data Generators",
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    install_requires=["aiofiles>=22.1.0"],
    python_requires=">=3.10"
)
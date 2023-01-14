from setuptools import find_packages, setup

setup(
    name="nu_csvparser",
    packages=find_packages(include=["nu_csvparser"]),
    version="0.1.0",
    description="Python Library to parse Nubank CSVs",
    author="Gabriel Berthier",
    install_requires=["aiofiles", "numpy", "pandas"],
    setup_requires=["pytest-runner"],
    tests_require=["pytest==4.4.1"],
    license="MIT",
)

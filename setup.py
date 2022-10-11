from setuptools import setup, find_packages

authors = [
    "Daniel Kornev <daniel@kornevs.org>",
    "Rami Mahskouk <rami.n.mashkouk@gmail.com>",
    "Maxim Talimanchuk <mtalimanchuk@gmail.com>",
]

install_requires = [
    "neomodel==4.0.8",
    "pydantic[dotenv]==1.9.0",
    "fabulist==1.2.0",
    "mimesis==5.3.0",
    "pylint==2.13.8",
    "treelib==1.6.1",
    "terminusdb_client @ git+https://github.com/terminusdb/terminusdb-client-python.git@fdb9a73#egg=terminusdb_client",
]

setup(
    name="deeppavlov-kg",
    author=", ".join(authors),
    version="0.0.1",
    packages=find_packages(),
    include_package_data=True,
    install_requires=install_requires,
)

from setuptools import setup, find_packages

authors = [
    "Daniel Kornev <daniel@kornevs.org>",
    "Rami Mahskouk <rami.n.mashkouk@gmail.com>",
    "Maxim Talimanchuk <mtalimanchuk@gmail.com>",
]

# with open("requirements.txt", "r", encoding="utf-8") as req_f:
#     install_requires = [line.strip() for line in req_f if line.strip()]

install_requires = [
    "neomodel==4.0.8",
    "pydantic[dotenv]==1.9.0",
    "fabulist==1.2.0",
    "mimesis==5.3.0",
    "pylint==2.13.8",
    "treelib==1.6.1",
    "terminusdb_client @ git+https://github.com/deeppavlov/terminusdb-client-python.git#egg=terminusdb_client",
]

setup(
    name="deeppavlov-kg",
    author=", ".join(authors),
    version="0.1.1",
    packages=find_packages(),
    include_package_data=True,
    install_requires=install_requires,
)

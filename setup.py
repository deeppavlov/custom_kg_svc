from setuptools import setup, find_packages

authors = [
    "Daniel Kornev <daniel@kornevs.org>",
    "Rami Mahskouk <rami.n.mashkouk@gmail.com>",
    "Maxim Talimanchuk <mtalimanchuk@gmail.com>",
]

with open("requirements.txt", "r", encoding="utf-8") as req_f:
    install_requires = [line.strip() for line in req_f if line.strip()]

setup(
    name="deeppavlov-kg",
    author=", ".join(authors),
    version="0.1.1",
    packages=find_packages(),
    include_package_data=True,
    install_requires=install_requires,
)

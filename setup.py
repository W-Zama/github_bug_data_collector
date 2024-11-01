from setuptools import setup, find_packages

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='your_package_name',
    version='0.1.0',
    packages=find_packages(),
    install_requires=requirements,  # requirements.txtの内容を利用
)

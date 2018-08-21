from setuptools import find_packages, setup

setup(
    name='pyathena',
    author="https://github.com/chaokunyang",
    version='1.0',
    description='athena python',
    long_description="athena python lib",
    classifiers=[
        'Programming Language :: Python :: 3.6',
        'Intended Audience :: Developers',
    ],
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False
)
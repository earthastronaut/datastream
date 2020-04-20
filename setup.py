from setuptools import setup

with open('./README.md') as f:
    long_description = f.read()

with open('./datastream/__version__') as f:
    version = f.read().rstrip()

setup(
    name='datastream',
    version=version,
    author='Dylan Gregersen',
    author_email='an.email0101@gmail.com',
    url='https://github.com/earthastronaut/datastream',
    license='MIT',
    description='A simple publish-subscribe messaging system using postgres as a backend.',
    long_description=long_description,
    install_requires=[
        "psycopg2>=2.8"
    ],
    python_requires='>=3.7',
    py_modules=[
        'datastream'
    ],
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3',
    ]
)

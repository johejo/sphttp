from setuptools import setup, find_packages

try:
    with open('README.md') as f:
        readme = f.read()
except IOError:
    readme = ''

setup(
    name='sphttp',
    version='0.1.0',
    author='Mitsuo Heijo',
    author_email='mitsuo_h@outlook.com',
    description='HTTP split downloader for Python supporting HTTP/2',
    long_description=readme,
    packages=find_packages(),
    license='MIT',
    url='http://github.com/johejo/sphttp',
    py_modules=['sphttp'],
    keywords=['HTTP', 'HTTP/2', 'http/2', 'http-client', 'multi-http'],
    install_requires=[
        'hyper>=0.7.0',
        'requests>=2.18.4',
        'yarl>=0.14.2',
        'aiohttp>=2.3.6',
    ],

    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: POSIX',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
    ]
)

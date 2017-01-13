from setuptools import setup, find_packages
import os

moduleDirectory = os.path.dirname(os.path.realpath(__file__))
exec(open(moduleDirectory + "/sherlock/__version__.py").read())


def readme():
    with open(moduleDirectory + '/README.rst') as f:
        return f.read()

install_requires = [
    'pyyaml',
    'sherlock',
    'fundamentals',
    'astrocalc',
    'neddy',
    'sloancone',
    'HMpTy',
    'numpy'
]

# READ THE DOCS SERVERS
exists = os.path.exists("/home/docs/")
if exists:
    c_exclude_list = ['healpy', 'astropy',
                      'numpy', 'sherlock', 'wcsaxes', 'HMpTy', 'ligo-gracedb']
    for e in c_exclude_list:
        try:
            install_requires.remove(e)
        except:
            pass

setup(name="sherlock",
      version=__version__,
      description="A python package and command-line tools to contextually classify astronomical transient sources. Sherlock mines a library of historical and on-going survey data to attempt to identify the source of a transient event, and predict the classification of the event based on the associated crossmatched data",
      long_description=readme(),
      classifiers=[
          'Development Status :: 4 - Beta',
          'License :: OSI Approved :: MIT License',
          'Programming Language :: Python :: 2.7',
          'Topic :: Utilities',
      ],
      keywords=['transients, astronomy, classifier, catalogues'],
      url='https://github.com/thespacedoctor/sherlock',
      download_url='https://github.com/thespacedoctor/sherlock/archive/v%(__version__)s.zip' % locals(
      ),
      author='David Young',
      author_email='davidrobertyoung@gmail.com',
      license='MIT',
      packages=find_packages(),
      include_package_data=True,
      install_requires=install_requires,
      test_suite='nose.collector',
      tests_require=['nose', 'nose-cover3'],
      entry_points={
          'console_scripts': [
              'sherlock=sherlock.cl_utils:main'
          ],
      },
      zip_safe=False)

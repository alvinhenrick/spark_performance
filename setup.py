from os import path, getcwd

from setuptools import setup, find_packages

package_name = 'spark_performance'

try:
    with open(path.join(getcwd(), 'VERSION')) as version_file:
        version = version_file.read().strip()
except IOError:
    raise


def parse_requirements(file):
    with open(file, "r") as fs:
        return [r for r in fs.read().splitlines() if
                (len(r.strip()) > 0 and not r.strip().startswith("#") and not r.strip().startswith("--"))]


requirements = parse_requirements('requirements.txt')
test_requirements = parse_requirements('requirements-dev.txt')

setup(name=package_name,
      version=version,
      license='',
      description='Project to run quick spark ml',
      author='Alvin Henrick',
      author_email='share.code@aol.com',
      url='https://github.com/alvinhenrick/spark_performance',
      packages=find_packages(exclude=['tests']),
      install_requires=requirements,
      tests_require=test_requirements,
      include_package_data=True,
      zip_safe=False)

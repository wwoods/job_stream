Release
=======

1. Edit sphinx/changelog.rst
2. Edit python/job_stream/version.py
3. Run all C++ tests: cd build; cmake .. && make -j8 test
    1. Note that it may be necessary to use `LD_LIBRARY_PATH=/path/to/boost/libs bash -c "cmake .. && make -j8 test"`.
       Note that LIBRARY_PATH will be inferred from LD_LIBRARY_PATH.
4. Run all Python tests: make -j8 test-python
    1. You SHOULD NOT use the above LD_LIBRARY_PATH trick.  Instead be using miniconda w/ conda install boost.
    2. Currently expected that the Python test job_stream.test.test_pipes.TestPipes fails
5. cd sphinx/ and make html, check documentation
6. Run python setup.py sdist
7. Run tar -ztvf dist/job_stream-latest.tar.gz, make sure only desired files were included
8. Commit changes to git except sphinx/\_build, reset those
9. Run python setup.py sdist upload -r pypi
10. cd sphinx/ and make gh-pages
11. Tag the python release (e.g., git tag python-v0.1.30)


Tests in Weird Locations
========================

Tests are easier to write in Python.  Therefore, some of the core C++ library functionality is actually tested in the Python test suite.


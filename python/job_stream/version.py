# From http://stackoverflow.com/questions/458550/standard-way-to-embed-version-into-python-package?noredirect=1&lq=1
# Store the version here so:
# 1) we don't load dependencies by storing it in __init__.py
# 2) we can import it into setup.py for the same reason
# 3) we can import it into our module
__version__ = '0.1.25'


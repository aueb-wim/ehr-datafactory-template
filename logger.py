import logging
import sys

# Create logger
LOGGER = logging.getLogger('datafactory')
LOGGER.setLevel(logging.DEBUG)
C_HANDLER = logging.StreamHandler(sys.stdout)
C_FORMAT = logging.Formatter('%(name)s: %(levelname)s: %(message)s')
C_HANDLER.setFormatter(C_FORMAT)
LOGGER.addHandler(C_HANDLER)
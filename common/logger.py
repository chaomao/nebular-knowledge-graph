#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging.config
import codecs
from common import constants

# logger
logging.config.fileConfig(constants.PATH_PREFIX + "config/logging.conf")
logger = logging.getLogger("data2kg")
# print a log
logger.info('Init logger config success.')

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import configparser
from common.logger import logger
from common import constants


class MyConfigParser(configparser.ConfigParser):
    """
    set configParse options for case-sensitive
    """
    def __int__(self, defaults=None):
        configparser.ConfigParser.__init__(self, defaults=defaults)

    def optionxform(self, optionstr: str) -> str:
        return optionstr


# db config
db_config_file = "../config/db_config.ini"
db_config = MyConfigParser()
db_filename = db_config.read(db_config_file, encoding='utf-8')

env = db_config.get(constants.ENV_CONF, constants.ENV_CHOOSE)
logger.info("now diet_kg_constructor env is:【" + env + "】")

# construct the section string
nebula_db_keywords_str = db_config.get(constants.ENV_CONF, constants.NEBULA_DB_KEYWORDS)

# nebula db config section string
NEBULA_DB_CONF = env + '_' + nebula_db_keywords_str

# tag_schema
tag_schema_file = "../config/tag_schema.txt"
tag_config = MyConfigParser()
tag_filename = tag_config.read(tag_schema_file, encoding='utf-8')

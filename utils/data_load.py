#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import os
import json
from common.logger import logger
from common import constants

line_num = 1000


def read_short_file_lines(small_file):
    """
    read short line file lines method.
    param small_file: small file path and file name
    :return: line list
    """
    line_list = []
    try:
        with open(small_file) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                line_list.append(line)
    except FileNotFoundError as e:
        logger.error('can not found the file: {}'.format(small_file))
        logger.exception(e)
    except LookupError:
        logger.exception('{} unknown coding error !'.format(small_file))
    except UnicodeDecodeError as e:
        logger.error('reading file {} fail when decoding the file!'.format(small_file))
        logger.exception(e)
    except Exception as e:
        logger.error('file {} reading error'.format(small_file))
        logger.exception(e)
    return line_list


def read_big_file_lines(big_file, encoding=constants.UTF_8_STR):
    """
    read big file line by line
    :param encoding:  编码方式
    :param big_file: big file path and file name
    :return: an iterator of the file lines
    """
    try:
        file = open(big_file, constants.READ_ONLY_STR, encoding=encoding)
        while 1:
            lines = file.readlines(line_num)
            if not lines:
                break
            yield from lines
    except FileNotFoundError as e:
        logger.error('can not found the file: {}'.format(big_file))
        logger.exception(e)
    except LookupError:
        logger.exception('{} unknown coding error !'.format(big_file))
    except UnicodeDecodeError as e:
        logger.error('reading file {} fail when decoding the file!'.format(big_file))
        logger.exception(e)
    except Exception as e:
        logger.error('file {} reading error'.format(big_file))
        logger.exception(e)
    return None


def make_dir_only(save_file):
    """
    make the directory only
    :param save_file: a save file
    """
    file_token = save_file.split("/")
    file_name = file_token[len(file_token) - 1]
    file_dir = save_file.split(file_name)[0]
    if not os.path.isdir(file_dir):
        os.makedirs(file_dir)
    if not os.path.exists(file_name):
        os.system(r'touch %s' % file_name)


def read_records_from_file(file):
    """
    read records from file
    :param file:
    :return:
    """
    lines = read_big_file_lines(file)
    header = []
    record_list = []
    for line in lines:
        line = line.rstrip('\n')
        if line.startswith('#'):
            header = line.replace('#', '').lstrip().split('\t')
            continue

        tokens = line.split('\t')
        adict = dict(zip(header, tokens))
        record_list.append(adict)

    return record_list


def get_json_file_data(file_path):
    dict_list = []
    with open(file_path, 'r', encoding='utf-8') as read_file:
        for line in read_file.readlines():
            line = line.strip()
            dict_list.append(json.loads(line))
    return dict_list

#!/usr/bin/env python3
# coding=utf-8


from nebula2.Config import Config
from nebula2.gclient.net import ConnectionPool

# 可不修改代码直接切换nebula3（pip install nebula3-python==3.3.0）
# from nebula3.Config import Config
# from nebula3.gclient.net import ConnectionPool

from common import conf
from common import constants
from common.logger import logger


class DBDriver:
    """
    DB Driver
    """
    def __init__(self, nebula_connection_pool_size):
        """
        init the driver
        :param nebula_connection_pool_size:
        """
        self._nebula_config = Config()
        self._nebula_config.max_connection_pool_size = nebula_connection_pool_size
        self.nebula_connection_pool, _ = self.__init_nebula()

    def __init_nebula(self):
        """
        init nebula pool
        :return: connection_pool and status
        """
        connection_pool = None
        ok = False
        try:
            # init connection pool
            connection_pool = ConnectionPool()
            # if connect success return true, else false.
            ok = connection_pool.init([(conf.db_config.get(conf.NEBULA_DB_CONF, constants.HOST),
                                        conf.db_config.getint(conf.NEBULA_DB_CONF, constants.PORT))],
                                      self._nebula_config)
            print('nebula host is ' + conf.db_config.get(conf.NEBULA_DB_CONF, constants.HOST))
            print('nebula port is ' + conf.db_config.get(conf.NEBULA_DB_CONF, constants.PORT))
            logger.info('Driver init nebula connection pool status is {}'.format(ok))
            print('Driver init nebula connection pool status is {}'.format(ok))
        except Exception as err:
            logger.error('Driver init nebula connection fail!')
            logger.exception(err)
        return connection_pool, ok

    def exec_one_nebula_ql_nebula(self, nebula_ql, use_space, is_create_space=False):
        """
        execute one ngql to nebula
        :param use_space:
        :param nebula_ql:
        :param is_create_space: False for not ,true for create a space
        :return:
        """
        result = None
        try:
            with self.nebula_connection_pool.\
                    session_context(conf.db_config.get(conf.NEBULA_DB_CONF, constants.USERNAME),
                                    conf.db_config.get(conf.NEBULA_DB_CONF, constants.PASSWORD)) \
                    as session:
                if not is_create_space:
                    session.execute("use " + use_space)
                result = session.execute(nebula_ql)
        except Exception as err:
            logger.error("execute error nebula_ql:  " + str(nebula_ql))
            logger.exception(err)
        return result

    def exec_many_nebula_ql_nebula(self, nebula_ql_list, use_space):
        """
        execute one nebula_ql to nebula
        :param nebula_ql_list:
        :param use_space:
        :return: result list all result list
        """
        result_list = []
        try:
            with self.nebula_connection_pool.\
                    session_context(conf.db_config.get(conf.NEBULA_DB_CONF, constants.USERNAME),
                                    conf.db_config.get(conf.NEBULA_DB_CONF, constants.PASSWORD)) \
                    as session:
                session.execute("use " + use_space + ";")
                for nebula_ql in nebula_ql_list:
                    result = session.execute(nebula_ql)
                    result_list.append(result)
        except Exception as err:
            logger.error("execute error nebula_ql list " + str(nebula_ql_list))
            logger.exception(err)
        return result_list

    def scan_all_vertex(self, space, return_props):
        """
        scan all the vertext
        :param space:
        :param return_props:
        :return:
        """
        pass

    def close(self):
        """
        close the resource
        """
        try:
            self.nebula_connection_pool.close()
            logger.info('Driver close nebula connection pool success')
        except Exception as err:
            logger.error('Driver close nebula connection pool  fail')
            logger.exception(err)


if __name__ == '__main__':
    driver = DBDriver(10)
    driver.close()

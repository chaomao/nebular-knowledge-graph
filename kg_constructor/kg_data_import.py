#!/usr/bin/env python3
# coding=utf-8


import time

# import sys
# sys.path.append("src")

from common import conf
from utils.data_load import *
from driver import nebula_driver


def create_space(n_driver, space_name, partition_num, replica_factor, vid_type, comment):
    """
    :param n_driver:
    :param space_name:
    :param partition_num:
    :param replica_factor:
    :param vid_type:
    :param comment:
    :return:
    """
    nebula_ql = 'CREATE SPACE {} (partition_num={}, replica_factor={}, vid_type={}) comment = \"{}\"' \
        .format(space_name, partition_num, replica_factor, vid_type, comment)
    conf.logger.debug(nebula_ql)
    n_driver.exec_one_nebula_ql_nebula(nebula_ql, space_name, is_create_space=True)


def create_tags_schema_to_kg(n_driver, kg_space):
    """
    :param n_driver:
    :param kg_space:
    :return: nebula_ql_list, result_list
    """
    tag_list = conf.tag_config.sections()
    nebula_ql_list = []
    for tag in tag_list:
        prop_type = conf.tag_config.items(tag)
        nebula_ql = "CREATE TAG IF NOT EXISTS " + tag + " ("

        for prop, data_desc in prop_type:
            data_desc = eval(data_desc)
            nebula_ql += prop + " " + data_desc[constants.DATA_TYPE] + " "
            if data_desc[constants.CAN_BE_NULL] and data_desc[constants.CAN_BE_NULL] == constants.NOT_NULL:
                nebula_ql += constants.NOT_NULL + " "

            if data_desc[constants.DEFAULT_VALUE]:
                nebula_ql += "DEFAULT " + data_desc[constants.DEFAULT_VALUE] + " "

            if data_desc[constants.COMMENT]:
                nebula_ql += "COMMENT '" + data_desc[constants.COMMENT] + "', "
        nebula_ql = nebula_ql[:-2]
        nebula_ql += ");"
        nebula_ql_list.append(nebula_ql)

    conf.logger.debug(nebula_ql_list)
    result_list = n_driver.exec_many_nebula_ql_nebula(nebula_ql_list, use_space=kg_space)
    return nebula_ql_list, result_list


def create_edge_schema_to_kg(n_driver, kg_space):
    """
    :param n_driver:
    :param kg_space:
    :return:
    """
    place_of_origin_edge_sql = 'CREATE edge placeOfOriginEdge();'
    brand_sql = 'CREATE edge brandEdge();'
    toy_type_sql = 'CREATE edge toyTypeEdge();'
    commodity_category_sql = 'CREATE edge commodityCategoryEdge();'
    pants_classification_sql = 'CREATE edge pantsClassificationEdge();'
    language_sql = 'CREATE edge languageEdge();'

    nebula_ql_list = [place_of_origin_edge_sql, brand_sql, toy_type_sql, commodity_category_sql,
                      pants_classification_sql, language_sql]
    conf.logger.debug(nebula_ql_list)
    result_list = n_driver.exec_many_nebula_ql_nebula(nebula_ql_list, use_space=kg_space)
    return nebula_ql_list, result_list


def create_common_nebula_ql_prefix(tag_name, prop_trans_dict=None, remove_prop_set=None):
    """
    :param tag_name:
    :param prop_trans_dict:
    :param remove_prop_set:
    :return: common nebula_ql output property
    """
    if prop_trans_dict is None:
        prop_trans_dict = {}
    if remove_prop_set is None:
        remove_prop_set = set()
    props = conf.tag_config.items(tag_name)
    if len(props) == 0:
        return
    output_prop = []
    prop_field = ""
    for p, _ in props:
        if p in remove_prop_set:
            continue
        prop_field += p + ", "
        if p in prop_trans_dict:
            p = prop_trans_dict[p]
        output_prop.append(p)

    common_sql = "INSERT VERTEX " + tag_name + " ("

    common_sql += prop_field
    common_sql = common_sql[:-2] + ") VALUES "
    return common_sql, output_prop


def get_int_and_float_prop_set(tag):
    """
    :param tag: tag name
    :return: int_props, float_props
    """
    prop_type = conf.tag_config.items(tag)
    int_props = set()
    float_props = set()
    # get the int and float props
    for prop, data_desc in prop_type:
        data_desc = eval(data_desc)
        if data_desc[constants.DATA_TYPE] == 'int':
            int_props.add(prop)
        if data_desc[constants.DATA_TYPE] == 'float':
            float_props.add(prop)

    return int_props, float_props


def create_local_dict_without_edge_to_kg(n_driver, record_list, tag_name, vid_prop, kg_space):
    """
    :param n_driver:
    :param record_list: local record list, element is adict
    :param tag_name: the tag name
    :param vid_prop: the vid prop
    :param kg_space:
    :return:
    """
    common_nebula, output_prop = create_common_nebula_ql_prefix(tag_name, {}, set())
    int_prop_set, float_prop_set = get_int_and_float_prop_set(tag_name)
    nebula_ql = common_nebula
    for record in record_list:
        for prop in output_prop:
            if prop == vid_prop:
                nebula_ql += '\"{}\":(\"{}\", '.format(record[prop], record[prop])
            elif prop in int_prop_set or prop in float_prop_set:
                nebula_ql += record.get(prop, '') + ', '
            else:
                nebula_ql += '\"{}\", '.format(record.get(prop, ''))
        nebula_ql = nebula_ql[:-2] + "), "
    nebula_ql = nebula_ql[:-2] + ";"
    conf.logger.debug(nebula_ql)
    n_driver.exec_one_nebula_ql_nebula(nebula_ql, use_space=kg_space)


def create_edge_to_kg(kg_driver, src_des_list, edge_type, kg_space, batch_size=constants.BATCH_SIZE):
    """
    :param batch_size:
    :param kg_space:
    :param kg_driver:
    :param src_des_list:
    :param edge_type:
    :return:
    """
    common_nebula_ql = 'INSERT EDGE {}() VALUES '.format(edge_type)
    values = ""
    index = 0
    nebula_ql_list = []
    for src_des in src_des_list:
        src_id, des_id = src_des.split("##")
        values += '\"{}\"->\"{}\":(), '.format(src_id, des_id)
        index += 1
        if index % batch_size == 0:
            nebula_ql = common_nebula_ql + values[:-2] + ';'
            nebula_ql_list.append(nebula_ql)
            index = 0
            values = ""

            if len(nebula_ql_list) == batch_size:
                conf.logger.debug(nebula_ql_list)
                kg_driver.exec_many_ngqls_nebula(nebula_ql_list, use_space=kg_space)
                nebula_ql_list = []

    # have left edges
    if index > 0:
        nebula_ql = common_nebula_ql + values[:-2] + ';'
        nebula_ql_list.append(nebula_ql)

    # have left nebula_ql_list
    if len(nebula_ql_list) > 0:
        conf.logger.debug(nebula_ql_list)
        kg_driver.exec_many_nebula_ql_nebula(nebula_ql_list, use_space=kg_space)


def rebuild_indexes(n_driver, index_name_list, kg_space, index_type='TAG'):
    """
    :param n_driver: the driver
    :param index_type: index type , TAG or EDGE
    :param index_name_list: index name list
    :param kg_space: use the space.
    :return:
    """
    nebula_ql = "REBUILD {} index ".format(index_type)
    for index_name in index_name_list:
        nebula_ql += index_name + ', '
    nebula_ql = nebula_ql[:-2]
    conf.logger.debug(nebula_ql)
    n_driver.exec_one_nebula_ql_nebula(nebula_ql, use_space=kg_space)


def create_normal_tag_to_kg(n_driver, data_file_path, tag_name, vid_prop, index_name, index_property, kg_space):
    """
    :param index_property:
    :param index_name:
    :param vid_prop:
    :param tag_name:
    :param data_file_path:
    :param n_driver:
    :param kg_space:
    :return:
    """
    data_record_list = read_records_from_file(data_file_path)
    create_local_dict_without_edge_to_kg(driver, data_record_list, tag_name, vid_prop, kg_space)
    index_nebula_ql_1 = "CREATE TAG INDEX " + index_name + " on " + tag_name + "(" + index_property + "(30)); "
    index_nebula_ql_list = [index_nebula_ql_1]
    conf.logger.debug(index_nebula_ql_list)
    n_driver.exec_many_nebula_ql_nebula(index_nebula_ql_list, use_space=kg_space)
    rebuild_indexes(n_driver, [index_name], kg_space)


def create_core_tag_to_kg(n_driver, file_path, tag_name, vid_prop):
    common_sql, output_prop = create_common_nebula_ql_prefix(tag_name)
    place_of_origin_edge_list = []
    brand_edge_list = []
    commodity_category_edge_list = []
    toy_type_edge_list = []
    pants_classification_edge_list = []
    language_edge_list = []
    commodity_json_list = get_json_file_data(file_path)
    for index in range(0, len(commodity_json_list), constants.BATCH_SIZE):
        nebula_ql = common_sql
        commodity_list = commodity_json_list[index:index + constants.BATCH_SIZE]
        for commodity_info in commodity_list:
            prop_info = commodity_info[constants.KEY_PROP]
            vertex_id = prop_info[vid_prop]
            rel_info = commodity_info[constants.KEY_REL]
            if rel_info:
                for rel_name, rel_target in rel_info.items():
                    if rel_name == 'placeOfOrigin':
                        place_of_origin_edge_list.append(vertex_id + "##" + rel_target)
                    elif rel_name == 'brand':
                        brand_edge_list.append(vertex_id + "##" + rel_target)
                    elif rel_name == 'commodityCategory':
                        commodity_category_edge_list.append(vertex_id + "##" + rel_target)
                    elif rel_name == 'toyType':
                        toy_type_edge_list.append(vertex_id + "##" + rel_target)
                    elif rel_name == 'pantsClassification':
                        pants_classification_edge_list.append(vertex_id + "##" + rel_target)
                    elif rel_name == 'language':
                        language_edge_list.append(vertex_id + "##" + rel_target)

            nebula_ql += '\"{}\":('.format(vertex_id)

            # 遍历属性
            for prop in output_prop:
                if prop not in prop_info:
                    nebula_ql += '"", '
                    continue
                if isinstance(prop_info[prop], int) or isinstance(prop_info[prop], float):
                    nebula_ql += str(round(prop_info[prop], 2)) + ', '
                else:
                    prop_value = prop_info[prop]
                    if prop_value == 'NULL':
                        nebula_ql += '"", '
                    else:
                        nebula_ql += '"' + prop_value + '", '
            nebula_ql = nebula_ql[:-2] + '), '

        nebula_ql = nebula_ql[:-2] + ";"

        # 执行 Tag 写入
        n_driver.exec_one_nebula_ql_nebula(nebula_ql, use_space=use_space)

        # 写入边信息
        create_edge_to_kg(n_driver, place_of_origin_edge_list, 'placeOfOriginEdge',
                          batch_size=constants.BATCH_SIZE, kg_space=use_space)
        place_of_origin_edge_list = []

        create_edge_to_kg(n_driver, brand_edge_list, 'brandEdge',
                          batch_size=constants.BATCH_SIZE, kg_space=use_space)
        brand_edge_list = []

        create_edge_to_kg(n_driver, commodity_category_edge_list, 'commodityCategoryEdge',
                          batch_size=constants.BATCH_SIZE, kg_space=use_space)
        commodity_category_edge_list = []

        create_edge_to_kg(n_driver, toy_type_edge_list, 'toyTypeEdge',
                          batch_size=constants.BATCH_SIZE, kg_space=use_space)
        toy_type_edge_list = []

        create_edge_to_kg(n_driver, pants_classification_edge_list, 'pantsClassificationEdge',
                          batch_size=constants.BATCH_SIZE, kg_space=use_space)
        pants_classification_edge_list = []

        create_edge_to_kg(n_driver, language_edge_list, 'languageEdge',
                          batch_size=constants.BATCH_SIZE, kg_space=use_space)
        language_edge_list = []

    index_sql = "CREATE TAG INDEX commodity_index on commodity(commodityId(64));"
    conf.logger.debug(index_sql)
    n_driver.exec_one_nebula_ql_nebula(index_sql, use_space=use_space)
    rebuild_indexes(n_driver, ["commodity_index"], use_space)


def construct_from_all(n_driver, space_name):
    """
    :param n_driver:
    :param space_name:
    :return:
    """

    begin_time = time.time()

    conf.logger.info("step1: create space...")
    create_space(n_driver, space_name=space_name, partition_num=10, replica_factor=1,
                 vid_type="FIXED_STRING(300)", comment='运动图谱空间')
    conf.logger.info('step1 [create space] finish')
    time.sleep(10)

    conf.logger.info('step2: create_tags_schema...')
    create_tags_schema_to_kg(driver, kg_space=space_name)
    conf.logger.info('step2 [create_tags_schema] finish')
    time.sleep(10)

    conf.logger.info('step3: create_edge_schema_to_kg')
    create_edge_schema_to_kg(driver, kg_space=space_name)
    conf.logger.info('step3 [create_edge_schema_to_kg] finish')
    time.sleep(10)

    # =========================== brand ===========================
    conf.logger.info('step4: create_normal_tag_to_kg')
    create_normal_tag_to_kg(n_driver, '../data/normal_tag_data/brand.csv', 'brand', 'brandId', 
                            'brand_index', 'brandId', use_space)
    conf.logger.info('step4 [create_normal_tag_to_kg] finish')
    time.sleep(10)

    # =========================== placeOfOrigin ===========================
    conf.logger.info('step5: create_normal_tag_to_kg')
    create_normal_tag_to_kg(n_driver, '../data/normal_tag_data/placeOfOrigin.csv', 'placeOfOrigin', 'placeOfOriginId',
                            'place_of_origin_index', 'placeOfOriginId', use_space)
    conf.logger.info('step5 [create_normal_tag_to_kg] finish')
    time.sleep(10)

    # =========================== commodityCategory ===========================
    conf.logger.info('step6: create_normal_tag_to_kg')
    create_normal_tag_to_kg(n_driver, '../data/normal_tag_data/commodityCategory.csv', 'commodityCategory',
                            'commodityCategoryId', 'commodity_category_index', 'commodityCategoryId', use_space)
    conf.logger.info('step6 [create_normal_tag_to_kg] finish')
    time.sleep(10)

    # =========================== toyType ===========================
    conf.logger.info('step7: create_normal_tag_to_kg')
    create_normal_tag_to_kg(n_driver, '../data/normal_tag_data/toyType.csv', 'toyType',
                            'toyTypeId', 'toy_type_index', 'toyTypeId', use_space)
    conf.logger.info('step7 [create_normal_tag_to_kg] finish')
    time.sleep(10)

    # =========================== pantsClassification ===========================
    conf.logger.info('step8: create_normal_tag_to_kg')
    create_normal_tag_to_kg(n_driver, '../data/normal_tag_data/pantsClassification.csv', 'pantsClassification',
                            'pantsClassificationId', 'pants_classification_index', 'pantsClassificationId', use_space)
    conf.logger.info('step8 [create_normal_tag_to_kg] finish')
    time.sleep(10)

    # =========================== language ===========================
    conf.logger.info('step9: create_normal_tag_to_kg')
    create_normal_tag_to_kg(n_driver, '../data/normal_tag_data/language.csv', 'language', 'languageId',
                            'language_index', 'languageId', use_space)
    conf.logger.info('step9 [create_normal_tag_to_kg] finish')
    time.sleep(10)

    # =========================== commodity ===========================
    conf.logger.info('step10: create_core_tag_to_kg')
    create_core_tag_to_kg(n_driver, '../data/core_tag_data/commodity.json', 'commodity', 'commodityId')
    conf.logger.info('step10 [create_core_tag_to_kg] finish')
    time.sleep(10)

    # =========================== create index ===========================
    conf.logger.info('step11: create index')
    index_sql = "CREATE TAG INDEX commodity_name on commodity(commodityName(64));"
    conf.logger.debug(index_sql)
    driver.exec_one_nebula_ql_nebula(index_sql, use_space=use_space)
    time.sleep(10)

    index_sql = "CREATE TAG INDEX commodity_brand_attribution on commodity(brandAttribution(64));"
    conf.logger.debug(index_sql)
    driver.exec_one_nebula_ql_nebula(index_sql, use_space=use_space)
    time.sleep(10)

    index_sql = "CREATE TAG INDEX brand_name_index on brand(brandName(64));"
    conf.logger.debug(index_sql)
    driver.exec_one_nebula_ql_nebula(index_sql, use_space=use_space)
    time.sleep(10)

    index_sql = "CREATE TAG INDEX toy_type_name_index on toyType(toyTypeName(64));"
    conf.logger.debug(index_sql)
    driver.exec_one_nebula_ql_nebula(index_sql, use_space=use_space)
    time.sleep(10)

    index_sql = "CREATE TAG INDEX place_of_origin_index on placeOfOrigin(placeOfOriginName(64));"
    conf.logger.debug(index_sql)
    driver.exec_one_nebula_ql_nebula(index_sql, use_space=use_space)
    time.sleep(10)

    # =========================== rebuild index ===========================
    rebuild_index_name_list = ['commodity_name', 'commodity_brand_attribution', 'brand_name_index',
                               'toy_type_name_index', 'place_of_origin_index']
    conf.logger.info("step12: rebuild index")
    conf.logger.info('rebuild index list: ' + str(rebuild_index_name_list))
    rebuild_indexes(driver, rebuild_index_name_list, kg_space=use_space)
    conf.logger.info('step12 [rebuild index] finish')

    end_time = time.time()
    conf.logger.info("total construct kg use time : " + str(end_time - begin_time))


if __name__ == '__main__':
    driver = nebula_driver.DBDriver(10)
    use_space = 'commodity_space'
    construct_from_all(driver, use_space)

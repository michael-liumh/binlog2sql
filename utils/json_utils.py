# !/usr/bin/env python3
# -*- coding:utf8 -*-
import logging
import colorlog
import re


# create a logger
logger = logging.getLogger('json_utils')
logger.setLevel(logging.INFO)

# set logger color
log_colors_config = {
    'DEBUG': 'bold_purple',
    'INFO': 'bold_green',
    'WARNING': 'bold_yellow',
    'ERROR': 'bold_red',
    'CRITICAL': 'red',
}

# set logger format
console_format = colorlog.ColoredFormatter(
    "[%(asctime)s] [%(module)s:%(funcName)s] [%(lineno)d] [%(levelname)s] %(log_color)s%(message)s",
    log_colors=log_colors_config
)

# add console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(console_format)
logger.addHandler(console_handler)


def fix_json_col(col_list):
    # 左括号数量 与 右括号数量
    json_mark_left_cnt = 0
    json_mark_right_cnt = 0
    json_col = ''
    col_list_new = []
    for i, col in enumerate(col_list):
        if isinstance(col, str):
            col = col.strip()

        if re.search('{', col) is not None:
            json_mark_left_cnt += col.count('{')
            if re.search('}', col) is not None:
                json_mark_right_cnt += col.count('}')
            json_col += col + ','
            if json_mark_left_cnt == json_mark_right_cnt:
                col_list_new.append(json_col[:-1])
                json_col = ''
        elif json_mark_left_cnt != json_mark_right_cnt:
            if re.search('}', col) is not None:
                json_mark_right_cnt += col.count('}')
            json_col += col + ','
            if json_mark_left_cnt == json_mark_right_cnt:
                col_list_new.append(json_col[:-1])
                json_col = ''
        else:
            col_list_new.append(col)
    return col_list_new

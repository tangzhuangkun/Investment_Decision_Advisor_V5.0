#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun

class MuteTargetVo:
    '''
    对标的物进行屏蔽
    所需用到的参数
    '''

    def __init__(self, target_type,target_code):
        self._target_type = target_type
        self._target_code = target_code

    # 标的类型
    @property
    def target_type(self):
        return self._target_type

    @target_type.setter
    def target_type(self, target_type):
        self._target = target_type

    # 标的代码
    @property
    def target_code(self):
        return self._target_code
    @target_code.setter
    def target_code(self, target_code):
        self._target_code = target_code
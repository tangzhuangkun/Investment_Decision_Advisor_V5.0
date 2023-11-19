#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author: Tang Zhuangkun

class IndexSimilarityVo:
    '''
    计算指数相似度使用到的对象
    '''


    """
    :param, index_code, 指数代码
    :param, index_name, 指数名称
    :param, issuer, 指数开发公司
    :param, similarity, 相似度
    """
    def __init__(self, index_code, index_name, issuer, similarity=None):
        self.index_code = index_code
        self.index_name = index_name
        self.issuer = issuer
        self.similarity = similarity

    def __hash__(self):
        return hash((self.index_code, self.index_name, self.issuer, self.similarity))

    @property
    def index_code(self):
        return self.index_code

    @index_code.setter
    def index_code(self, index_code):
        self.index_code = index_code

    @property
    def index_name(self):
        return self.index_name

    @index_name.setter
    def index_name(self, index_name):
        self.index_name = index_name

    @property
    def issuer(self):
        return self.issuer

    @issuer.setter
    def issuer(self, issuer):
        self.issuer = issuer

    @property
    def similarity(self):
        return self.similarity

    @similarity.setter
    def similarity(self, similarity):
        self.similarity = similarity


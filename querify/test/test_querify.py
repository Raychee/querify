import re
from datetime import datetime

from ..querify import Expr, And, SchemaLiteral, RegexLiteral, IntLiteral, MatchRegex, StringLiteral, \
    InverseMatchRegex, \
    Or, DateTimeLiteral, \
    FloatLiteral, ClassFromJsonWithSubclassDictMeta, Select, ShowTagKeys, ShowColumns, EqualValue, NotEqualValue, \
    GreaterThanValue, GreaterThanOrEqualValue, LessThanValue, LessThanOrEqualValue, EqualField, NotEqualField, \
    GreaterThanField, GreaterThanOrEqualField, LessThanField, LessThanOrEqualField, Null, In, NotIn
from ..utility import deep_equal


def test_meta_class():
    class A(metaclass=ClassFromJsonWithSubclassDictMeta):
        pass

    class B(A):
        base = True

    class B1(B):
        key = 'B1'

    class B2(B):
        pass

    class B3(B):
        key = 'B3'

    class C(B):
        pass

    class D(C):
        base = True
        key = 'D'

    class D1(D):
        key = 'D1'

    class D2(D):
        key = 'D2'

    class E(D):
        base = True
        key = 'E'

    class E1(E):
        key = 'E1'

    assert not hasattr(A, 'subclasses')
    assert hasattr(B, 'subclasses')
    assert deep_equal(B.subclasses, {'B1': B1, 'B3': B3, 'D': D})
    assert hasattr(C, 'subclasses')
    assert deep_equal(C.subclasses, {'B1': B1, 'B3': B3, 'D': D})
    assert hasattr(D, 'subclasses')
    assert deep_equal(D.subclasses, {'D1': D1, 'D2': D2, 'E': E})
    assert hasattr(E, 'subclasses')
    assert deep_equal(E.subclasses, {'E1': E1})


# def test_normalize_query_json():
#     metric_filter = {
#         'rule_id': [6666, '7777', 8888],
#         'act_type': 'logging',
#         'expected_fire_volume': 10000,
#         'expected_fire_rate': 99.9,
#         '__or__': [
#             {
#                 'create_ts': {'__lte__': datetime(2015, 12, 31, 12, 5),
#                               '__gt__': datetime(2014, 1, 1, 0, 0)},
#                 'version': {'__lt__': 3, '__gte__': 1},
#             },
#             {'version': {'__eq__': 4}}
#         ],
#         '__and__': [
#             {'rule_name': '/logging_.*/'},
#             {'rule_name': {'__neq__': 'logging_rddms'}},
#             {'rule_name': {'__iregex__': 'logging_r..s'}}
#         ]
#     }
#
#     expr_dict = BooleanExpr.normalize_eval_expr_dict(metric_filter)
#     expected_expr_dict = {
#         '__and__': [
#             {
#                 '__or__': [
#                     {'rule_id': {'__eq__': 6666}},
#                     {'rule_id': {'__eq__': '7777'}},
#                     {'rule_id': {'__eq__': 8888}}
#                 ]
#             },
#             {'act_type': {'__eq__': 'logging'}},
#             {'expected_fire_volume': {'__eq__': 10000}},
#             {'expected_fire_rate': {'__eq__': 99.9}},
#             {
#                 '__or__': [
#                     {
#                         '__and__': [
#                             {'create_ts': {'__lte__': datetime(2015, 12, 31, 12, 5)}},
#                             {'create_ts': {'__gt__': datetime(2014, 1, 1, 0, 0)}},
#                             {'version': {'__lt__': 3}},
#                             {'version': {'__gte__': 1}},
#                         ]
#                     },
#                     {'version': {'__eq__': 4}}
#                 ]
#             },
#             {'rule_name': {'__regex__': 'logging_.*'}},
#             {'rule_name': {'__neq__': 'logging_rddms'}},
#             {'rule_name': {'__iregex__': 'logging_r..s'}}
#         ]
#     }
#     assert deep_equal(expr_dict, expected_expr_dict, unordered_list=True)


def test_empty_query():
    expr = Expr.from_json({})
    assert expr.to_query('influx') == ''
    assert expr.to_query('mysql') == ''
    assert expr.to_query('pandas') == ''
    assert expr.to_query('pluto') == ''
    assert deep_equal(expr.to_query('mongo'), {'$and': []})


def test_nested_empty_query():
    expr = Expr.from_json({
        '__and__': [
            {},
            {'__or__': [
                {
                    'version': {'__eq__': 4}
                }
            ]}
        ]
    })
    assert expr.to_query('influx') == '(("version" = 4))'
    assert expr.to_query('mysql') == '((version = 4))'
    assert expr.to_query('pandas') == '((version == 4))'
    assert expr.to_query('pluto') == '((version equals 4))'
    assert deep_equal(expr.to_query('mongo'), {'$and': [{'$or': [{'version': {'$eq': 4}}]}]})


def test_generate_query_for_influx():
    query_json = {
        'rule_id': [6666, '7777', 8888],
        'act_type': {'__nin__': ['logging', 'eval']},
        'expected_fire_volume': 10000,
        'expected_fire_rate': 99.9,
        'rule_owner': 'me',
        'rule_writer': {'__neqf__': 'rule_owner'},
        'last_modifier': {'__eqf__': 'rule_writer'},
        '__or__': [
            {
                'create_ts': {'__lte__': datetime(2015, 12, 31, 12, 5),
                              '__gt__': datetime(2014, 1, 1, 0, 0),
                              '__gtef__': 'update_ts',
                              '__ltf__': 'retire_ts'},
                'version': {'__lt__': 3, '__gte__': 1},
            },
            {
                'version': {'__eq__': 4},
                'expected_fire_volume': {'__ltef__': 'expected_max_volume',
                                         '__gtf__': 'expected_min_volume'},
                'country': {'__in__': ['UK', 'DE']}
            }
        ],
        '__and__': [
            {'rule_name': '/logging_.*/'},
            {'rule_name': {'__neq__': 'logging_rddms'}},
            {'rule_name': {'__iregex__': 'logging_r..s'}}
        ]
    }

    expr = Expr.from_json(query_json)
    assert expr.to_query('influx') == \
        """("rule_name" =~ /logging_.*/) AND ("rule_name" != 'logging_rddms') AND ("rule_name" !~ /logging_r..s/) AND ((("create_ts" > '2014-01-01T00:00:00Z') AND ("create_ts" >= "update_ts") AND ("create_ts" <= '2015-12-31T12:05:00Z') AND ("create_ts" < "retire_ts") AND ("version" >= 1) AND ("version" < 3)) OR ((("country" = 'UK') OR ("country" = 'DE')) AND ("expected_fire_volume" > "expected_min_volume") AND ("expected_fire_volume" <= "expected_max_volume") AND ("version" = 4))) AND (("act_type" != 'logging') AND ("act_type" != 'eval')) AND ("expected_fire_rate" = 99.9) AND ("expected_fire_volume" = 10000) AND ("last_modifier" = "rule_writer") AND (("rule_id" = 6666) OR ("rule_id" = '7777') OR ("rule_id" = 8888)) AND ("rule_owner" = 'me') AND ("rule_writer" != "rule_owner")"""


def test_generate_query_for_mysql():
    query_json = {
        'rule_id': [6666, '7777', 8888],
        'act_type': {'__nin__': ['logging', 'eval']},
        'expected_fire_volume': 10000,
        'expected_fire_rate': 99.9,
        'rule_owner': 'me',
        'rule_writer': {'__neqf__': 'rule_owner',
                        '__null__': False},
        'last_modifier': {'__eqf__': 'rule_writer'},
        '__or__': [
            {
                'create_ts': {'__lte__': datetime(2015, 12, 31, 12, 5),
                              '__gt__': datetime(2014, 1, 1, 0, 0),
                              '__gtef__': 'update_ts',
                              '__ltf__': 'retire_ts'},
                'version': {'__lt__': 3, '__gte__': 1},
            },
            {
                'version': {'__eq__': 4},
                'expected_fire_volume': {'__ltef__': 'expected_max_volume',
                                         '__gtf__': 'expected_min_volume'},
                'country': {'__in__': ['UK', 'DE']}
            }
        ],
        '__and__': [
            {'rule_name': '/logging_.*/'},
            {'rule_name': {'__neq__': 'logging_rddms'}},
            {'rule_name': {'__iregex__': 'logging_r..s'}}
        ]
    }

    expr = Expr.from_json(query_json)
    assert expr.to_query('mysql') == \
        """(rule_name REGEXP 'logging_.*') AND (rule_name <> 'logging_rddms') AND (rule_name NOT REGEXP 'logging_r..s') AND (((create_ts > '2014-01-01 00:00:00') AND (create_ts >= update_ts) AND (create_ts <= '2015-12-31 12:05:00') AND (create_ts < retire_ts) AND (version >= 1) AND (version < 3)) OR ((country IN ('UK', 'DE')) AND (expected_fire_volume > expected_min_volume) AND (expected_fire_volume <= expected_max_volume) AND (version = 4))) AND (act_type NOT IN ('logging', 'eval')) AND (expected_fire_rate = 99.9) AND (expected_fire_volume = 10000) AND (last_modifier = rule_writer) AND (rule_id IN (6666, '7777', 8888)) AND (rule_owner = 'me') AND (rule_writer <> rule_owner) AND (rule_writer is NOT NULL)"""


def test_generate_query_for_mongo():
    query_json = {
        'rule_id': [6666, '7777', 8888],
        'act_type': {'__nin__': ['logging', 'eval']},
        'expected_fire_volume': 10000,
        'expected_fire_rate': 99.9,
        'rule_owner': 'me',
        'rule_writer': {'__null__': False},
        '__or__': [
            {
                'create_ts': {'__lte__': datetime(2015, 12, 31, 12, 5),
                              '__gt__': datetime(2014, 1, 1, 0, 0)},
                'version': {'__lt__': 3, '__gte__': 1},
            },
            {
                'version': {'__eq__': 4},
                'country': {'__in__': ['UK', 'DE']}
            }
        ],
        '__and__': [
            {'rule_name': '/logging_.*/'},
            {'rule_name': {'__neq__': 'logging_rddms'}},
            {'rule_name': {'__iregex__': 'logging_r..s'}}
        ]
    }

    expr = Expr.from_json(query_json)

    assert deep_equal(expr.to_query('mongo'), {
        '$and': [
            {'rule_name': re.compile('logging_.*')},
            {'rule_name': {'$ne': 'logging_rddms'}},
            {'rule_name': {'$not': re.compile('logging_r..s')}},
            {'$or': [
                {'$and': [
                    {'create_ts': {'$gt': datetime(2014, 1, 1, 0, 0)}},
                    {'create_ts': {'$lte': datetime(2015, 12, 31, 12, 5)}},
                    {'version': {'$gte': 1}},
                    {'version': {'$lt': 3}}
                ]},
                {'$and': [
                    {'country': {'$in': ['UK', 'DE']}},
                    {'version': {'$eq': 4}}
                ]}
            ]},
            {'act_type': {'$nin': ['logging', 'eval']}},
            {'expected_fire_rate': {'$eq': 99.9}},
            {'expected_fire_volume': {'$eq': 10000}},
            {'rule_id': {'$in': [6666, '7777', 8888]}},
            {'rule_owner': {'$eq': 'me'}},
            {'rule_writer': {'$ne': None}}
        ]}, unordered_list=True)


def test_generate_query_for_pandas():
    query_json = {
        'rule_id': [6666, '7777', 8888],
        'act_type': {'__nin__': ['logging', 'eval']},
        'expected_fire_volume': 10000,
        'expected_fire_rate': 99.9,
        'rule_owner': 'me',
        'rule_writer': {'__neqf__': 'rule_owner',
                        '__null__': False},
        'last_modifier': {'__eqf__': 'rule_writer'},
        '__or__': [
            {
                'create_ts': {'__gtef__': 'update_ts',
                              '__ltf__': 'retire_ts'},
                'version': {'__lt__': 3, '__gte__': 1},
            },
            {
                'version': {'__eq__': 4},
                'expected_fire_volume': {'__ltef__': 'expected_max_volume',
                                         '__gtf__': 'expected_min_volume'},
                'country': {'__in__': ['UK', 'DE']}
            }
        ],
        '__and__': [
            {'rule_name': {'__neq__': 'logging_rddms'}},
        ]
    }

    expr = Expr.from_json(query_json)
    assert expr.to_query('pandas') == \
        """(rule_name != 'logging_rddms') & (((create_ts >= update_ts) & (create_ts < retire_ts) & (version >= 1) & (version < 3)) | (((country == 'UK') | (country == 'DE')) & (expected_fire_volume > expected_min_volume) & (expected_fire_volume <= expected_max_volume) & (version == 4))) & ((act_type != 'logging') & (act_type != 'eval')) & (expected_fire_rate == 99.9) & (expected_fire_volume == 10000) & (last_modifier == rule_writer) & ((rule_id == 6666) | (rule_id == '7777') | (rule_id == 8888)) & (rule_owner == 'me') & (rule_writer != rule_owner) & (~pandas.isnull(rule_writer))"""


def test_generate_query_for_pluto():
    query_json = {
        'rule_id': [6666, '7777', 8888],
        'act_type': {'__nin__': ['logging', 'eval']},
        'expected_fire_volume': 10000,
        'expected_fire_rate': 99.9,
        'rule_owner': 'me',
        'rule_writer': {'__neqf__': 'rule_owner',
                        '__null__': False},
        'last_modifier': {'__eqf__': 'rule_writer'},
        '__or__': [
            {
                'create_ts': {'__gtef__': 'update_ts',
                              '__ltf__': 'retire_ts'},
                'version': {'__lt__': 3, '__gte__': 1},
            },
            {
                'version': {'__eq__': 4},
                'expected_fire_volume': {'__ltef__': 'expected_max_volume',
                                         '__gtf__': 'expected_min_volume'},
                'country': {'__in__': ['UK', 'DE']}
            }
        ],
        '__and__': [
            {'rule_name': {'__neq__': 'logging_rddms'}},
        ]
    }

    expr = Expr.from_json(query_json)
    assert expr.to_query('pluto') == \
        """(rule_name does not equal "logging_rddms") and (((create_ts is at least update_ts) and (create_ts is less than retire_ts) and (version is at least 1) and (version is less than 3)) or ((("country" = 'UK') OR ("country" = 'DE')) and (expected_fire_volume is more than expected_min_volume) and (expected_fire_volume is at most expected_max_volume) and (version equals 4))) and (("act_type" != 'logging') AND ("act_type" != 'eval')) and (expected_fire_rate equals 99.9) and (expected_fire_volume equals 10000) and (last_modifier equals rule_writer) and (("rule_id" = 6666) OR ("rule_id" = '7777') OR ("rule_id" = 8888)) and (rule_owner equals "me") and (rule_writer does not equal rule_owner) and (rule_writer is not null)"""


def test_iter_exprs():
    query_json = {
        'rule_id': [6666, '7777', 8888],
        'act_type': {'__nin__': ['logging', 'eval']},
        'expected_fire_volume': 10000,
        'expected_fire_rate': 99.9,
        'rule_owner': 'me',
        'rule_writer': {'__neqf__': 'rule_owner',
                        '__null__': False},
        'last_modifier': {'__eqf__': 'rule_writer'},
        '__or__': [
            {
                'create_ts': {'__lte__': datetime(2015, 12, 31, 12, 5),
                              '__gt__': datetime(2014, 1, 1, 0, 0),
                              '__gtef__': 'update_ts',
                              '__ltf__': 'retire_ts'},
                'version': {'__lt__': 3, '__gte__': 1},
            },
            {
                'version': {'__eq__': 4},
                'expected_fire_volume': {'__ltef__': 'expected_max_volume',
                                         '__gtf__': 'expected_min_volume'},
                'country': {'__in__': ['UK', 'DE']}
            }
        ],
        '__and__': [
            {'rule_name': '/logging_.*/'},
            {'rule_name': {'__neq__': 'logging_rddms'}},
            {'rule_name': {'__iregex__': 'logging_r..s'}}
        ]
    }

    expr = Expr.from_json(query_json)
    all_sub_exprs = list(expr)
    assert len(all_sub_exprs) == 69
    assert len([e for e in all_sub_exprs if type(e) is And]) == 3
    assert len([e for e in all_sub_exprs if type(e) is Or]) == 1
    assert len([e for e in all_sub_exprs if type(e) is EqualValue]) == 4
    assert len([e for e in all_sub_exprs if type(e) is EqualField]) == 1
    assert len([e for e in all_sub_exprs if type(e) is NotEqualValue]) == 1
    assert len([e for e in all_sub_exprs if type(e) is NotEqualField]) == 1
    assert len([e for e in all_sub_exprs if type(e) is StringLiteral]) == 7
    assert len([e for e in all_sub_exprs if type(e) is IntLiteral]) == 6
    assert len([e for e in all_sub_exprs if type(e) is FloatLiteral]) == 1
    assert len([e for e in all_sub_exprs if type(e) is DateTimeLiteral]) == 2
    assert len([e for e in all_sub_exprs if type(e) is RegexLiteral]) == 2
    assert len([e for e in all_sub_exprs if type(e) is SchemaLiteral]) == 26
    assert len([e for e in all_sub_exprs if type(e) is MatchRegex]) == 1
    assert len([e for e in all_sub_exprs if type(e) is InverseMatchRegex]) == 1
    assert len([e for e in all_sub_exprs if type(e) is GreaterThanValue]) == 1
    assert len([e for e in all_sub_exprs if type(e) is GreaterThanField]) == 1
    assert len([e for e in all_sub_exprs if type(e) is GreaterThanOrEqualValue]) == 1
    assert len([e for e in all_sub_exprs if type(e) is GreaterThanOrEqualField]) == 1
    assert len([e for e in all_sub_exprs if type(e) is LessThanValue]) == 1
    assert len([e for e in all_sub_exprs if type(e) is LessThanField]) == 1
    assert len([e for e in all_sub_exprs if type(e) is LessThanOrEqualValue]) == 1
    assert len([e for e in all_sub_exprs if type(e) is LessThanOrEqualField]) == 1
    assert len([e for e in all_sub_exprs if type(e) is Null]) == 1
    assert len([e for e in all_sub_exprs if type(e) is In]) == 2
    assert len([e for e in all_sub_exprs if type(e) is NotIn]) == 1


def test_select_stmt():
    query_json = {
        'rule_id': [6666, '7777', 8888],
        'act_type': 'logging',
        'expected_fire_volume': 10000,
        'expected_fire_rate': 99.9,
        '__or__': [
            {
                'create_ts': {'__lte__': datetime(2015, 12, 31, 12, 5),
                              '__gt__': datetime(2014, 1, 1, 0, 0)},
                'version': {'__lt__': 3, '__gte__': 1},
            },
            {
                'version': {'__eq__': 4}
            }
        ],
        '__and__': [
            {'rule_name': '/logging_.*/'},
            {'rule_name': {'__neq__': 'logging_rddms'}},
            {'rule_name': {'__iregex__': 'logging_r..s'}}
        ]
    }

    assert Select(table='m', retention_policy='rp', db='db',
                  columns=['a', 'b'], where=query_json).to_query('influx') == \
        """SELECT "a","b" FROM "db"."rp"."m" WHERE ("rule_name" =~ /logging_.*/) AND ("rule_name" != 'logging_rddms') AND ("rule_name" !~ /logging_r..s/) AND ((("create_ts" > '2014-01-01T00:00:00Z') AND ("create_ts" <= '2015-12-31T12:05:00Z') AND ("version" >= 1) AND ("version" < 3)) OR ("version" = 4)) AND ("act_type" = 'logging') AND ("expected_fire_rate" = 99.9) AND ("expected_fire_volume" = 10000) AND (("rule_id" = 6666) OR ("rule_id" = '7777') OR ("rule_id" = 8888))"""
    assert Select(table='m', retention_policy='rp', db='db',
                  columns=['a', 'b'], where=query_json).to_query('mysql') == \
        """SELECT a,b FROM db.m WHERE (rule_name REGEXP 'logging_.*') AND (rule_name <> 'logging_rddms') AND (rule_name NOT REGEXP 'logging_r..s') AND (((create_ts > '2014-01-01 00:00:00') AND (create_ts <= '2015-12-31 12:05:00') AND (version >= 1) AND (version < 3)) OR (version = 4)) AND (act_type = 'logging') AND (expected_fire_rate = 99.9) AND (expected_fire_volume = 10000) AND (rule_id IN (6666, '7777', 8888))"""

    assert Select(table='m', db='db',
                  columns=[], where=query_json).to_query('influx') == \
        """SELECT * FROM "db".."m" WHERE ("rule_name" =~ /logging_.*/) AND ("rule_name" != 'logging_rddms') AND ("rule_name" !~ /logging_r..s/) AND ((("create_ts" > '2014-01-01T00:00:00Z') AND ("create_ts" <= '2015-12-31T12:05:00Z') AND ("version" >= 1) AND ("version" < 3)) OR ("version" = 4)) AND ("act_type" = 'logging') AND ("expected_fire_rate" = 99.9) AND ("expected_fire_volume" = 10000) AND (("rule_id" = 6666) OR ("rule_id" = '7777') OR ("rule_id" = 8888))"""
    assert Select(table='m', db='db',
                  columns=[], where=query_json).to_query('mysql') == \
        """SELECT * FROM db.m WHERE (rule_name REGEXP 'logging_.*') AND (rule_name <> 'logging_rddms') AND (rule_name NOT REGEXP 'logging_r..s') AND (((create_ts > '2014-01-01 00:00:00') AND (create_ts <= '2015-12-31 12:05:00') AND (version >= 1) AND (version < 3)) OR (version = 4)) AND (act_type = 'logging') AND (expected_fire_rate = 99.9) AND (expected_fire_volume = 10000) AND (rule_id IN (6666, '7777', 8888))"""

    assert Select(table='m').to_query('influx') == \
        'SELECT * FROM "m"'
    assert Select(table='m').to_query('mysql') == \
        'SELECT * FROM m'


def test_show_tag_keys():
    query_json = {
        'rule_id': [6666, '7777', 8888],
        'act_type': 'logging',
        'expected_fire_volume': 10000,
        'expected_fire_rate': 99.9,
        '__or__': [
            {
                'create_ts': {'__lte__': datetime(2015, 12, 31, 12, 5),
                              '__gt__': datetime(2014, 1, 1, 0, 0)},
                'version': {'__lt__': 3, '__gte__': 1},
            },
            {
                'version': {'__eq__': 4}
            }
        ],
        '__and__': [
            {'rule_name': '/logging_.*/'},
            {'rule_name': {'__neq__': 'logging_rddms'}},
            {'rule_name': {'__iregex__': 'logging_r..s'}}
        ]
    }

    assert ShowTagKeys(measurement='m', retention_policy='rp', db='db', where=query_json).to_query('influx') == \
        """SHOW TAG KEYS ON "db" FROM "rp"."m" WHERE ("rule_name" =~ /logging_.*/) AND ("rule_name" != 'logging_rddms') AND ("rule_name" !~ /logging_r..s/) AND ((("create_ts" > '2014-01-01T00:00:00Z') AND ("create_ts" <= '2015-12-31T12:05:00Z') AND ("version" >= 1) AND ("version" < 3)) OR ("version" = 4)) AND ("act_type" = 'logging') AND ("expected_fire_rate" = 99.9) AND ("expected_fire_volume" = 10000) AND (("rule_id" = 6666) OR ("rule_id" = '7777') OR ("rule_id" = 8888))"""
    assert ShowTagKeys(measurement='m').to_query('influx') == \
        'SHOW TAG KEYS FROM "m"'


def test_show_columns():
    assert ShowColumns(table='m', db='db').to_query('influx') == \
        'SHOW TAG KEYS ON "db" FROM "m"'
    assert ShowColumns(table='m', db='db').to_query('mysql') == \
        'SHOW COLUMNS FROM db.m'


def test_not_and_null():
    query_json = {
        '__or__': [
            {
                'version': {'__eq__': 4}
            }
        ],
        '__and__': [
            {'rule_name': {'__neq__': 'logging_rddms'}},
            {'__not__': {'rule_name': {'__eq__': 'invalid_name'}}},
            {'rule_body': {'__null__': False}}
        ]
    }

    expr = Expr.from_json(query_json)
    assert expr.to_query('mysql') == \
           "(rule_name <> 'logging_rddms') AND (NOT (rule_name = 'invalid_name')) AND (rule_body is NOT NULL) AND ((version = 4))"
    assert expr.to_query('pandas') == \
           "(rule_name != 'logging_rddms') & (~(rule_name == 'invalid_name')) & (~pandas.isnull(rule_body)) & ((version == 4))"
    assert deep_equal(expr.to_query('mongo'), {
        '$and': [
            {'rule_name': {'$ne': 'logging_rddms'}},
            {'rule_name': {'$not': {'$eq': 'invalid_name'}}},
            {'rule_body': {'$ne': None}},
            {'$or': [
                {'version': {'$eq': 4}}
            ]}
        ]}, unordered_list=True)


def test_null2():
    input_json = {
        '__or__': [
            {'__and__': [
                {'__or__': [
                    {'inconsistency_1': {'__lt__': 0.5}},
                    {'inconsistency_1': {'__null__': True}}]},
                {'__or__': [
                    {'latency': {'__lt__': 3.2}},
                    {'latency': {'__null__': True}}
                ]}
            ]}
        ]}

    e = Expr.from_json(input_json)
    assert e.to_query_pandas() == \
        "(((inconsistency_1 < 0.5) | (pandas.isnull(inconsistency_1))) & ((latency < 3.2) | (pandas.isnull(latency))))"


def test_missing():
    query_json = {
        '__or__': [
            {
                'version': {'__eq__': 4}
            }
        ],
        '__and__': [
            {'rule_name': {'__neq__': 'logging_rddms'}},
            {'rule_body': {'__missing__': True}}
        ]
    }

    expr = Expr.from_json(query_json)
    assert deep_equal(expr.to_query('mongo'), {
        '$and': [
            {'rule_name': {'$ne': 'logging_rddms'}},
            {'rule_body': {'$exists': False}},
            {'$or': [
                {'version': {'$eq': 4}}
            ]}
        ]}, unordered_list=True)


def test_pluto():
    #test: or, and, any,does not equal, equals,is at least, is less than, is at most, is more than
    query_json = {
        "__any__": [
            {"__and__": [
                {"inconsistency": {"__neq__": "low"}},
                {"latency": {"__lt__": 3.2}}
            ]},
            {"__and__": [
                {"inconsistency": {"__neq__": "low"}},
                {"latency": {"__gte__": 3.2}}
            ]},
            {"__and__": [
                {"inconsistency": {"__eq__": "low"}},
                {"__or__": [
                    {"latency": {"__lte__": -1}},
                    {"latency": {"__gt__": 100}}
                ]}
            ]}
        ]
    }

    expr = Expr.from_json(query_json)
    result = expr.to_query_pluto()

    assert result == \
        """any of the following conditions is true :
      - (inconsistency does not equal "low") and (latency is at least 3.2)
      - (inconsistency does not equal "low") and (latency is less than 3.2)
      - (inconsistency equals "low") and ((latency is at most -1) or (latency is more than 100))"""
    print(result)

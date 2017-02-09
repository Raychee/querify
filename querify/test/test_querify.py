from datetime import datetime

from ..utility import deep_equal
from ..querify import BooleanExpr, Expr, And, Equal, SchemaLiteral, RegexLiteral, IntLiteral, MatchRegex, NotEqual, \
    StringLiteral, InverseMatchRegex, Or, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, DateTimeLiteral, \
    FloatLiteral, ClassFromJsonWithSubclassDictMeta, Select, ShowTagKeys


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


def test_normalize_query_json():
    metric_filter = {
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
            {'version': {'__eq__': 4}}
        ],
        '__and__': [
            {'rule_name': '/logging_.*/'},
            {'rule_name': {'__neq__': 'logging_rddms'}},
            {'rule_name': {'__iregex__': 'logging_r..s'}}
        ]
    }

    expr_dict = BooleanExpr.normalize_eval_expr_dict(metric_filter)
    expected_expr_dict = {
        '__and__': [
            {
                '__or__': [
                    {'rule_id': {'__eq__': 6666}},
                    {'rule_id': {'__eq__': '7777'}},
                    {'rule_id': {'__eq__': 8888}}
                ]
            },
            {'act_type': {'__eq__': 'logging'}},
            {'expected_fire_volume': {'__eq__': 10000}},
            {'expected_fire_rate': {'__eq__': 99.9}},
            {
                '__or__': [
                    {
                        '__and__': [
                            {'create_ts': {'__lte__': datetime(2015, 12, 31, 12, 5)}},
                            {'create_ts': {'__gt__': datetime(2014, 1, 1, 0, 0)}},
                            {'version': {'__lt__': 3}},
                            {'version': {'__gte__': 1}},
                        ]
                    },
                    {'version': {'__eq__': 4}}
                ]
            },
            {'rule_name': {'__regex__': 'logging_.*'}},
            {'rule_name': {'__neq__': 'logging_rddms'}},
            {'rule_name': {'__iregex__': 'logging_r..s'}}
        ]
    }
    assert deep_equal(expr_dict, expected_expr_dict, unordered_list=True)


def test_generate_influxql():
    metric_filter = {
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

    expr = Expr.from_json(metric_filter)
    influx_query = expr.to_query()
    expected_influx_query = """("act_type" = 'logging') AND ("expected_fire_rate" = 99.9) AND ("expected_fire_volume" = 10000) AND ("rule_name" != 'logging_rddms') AND ("rule_name" !~ /logging_r..s/) AND ("rule_name" =~ /logging_.*/) AND (("rule_id" = '7777') OR ("rule_id" = 6666) OR ("rule_id" = 8888)) AND (("version" = 4) OR (("create_ts" <= '2015-12-31T12:05:00Z') AND ("create_ts" > '2014-01-01T00:00:00Z') AND ("version" < 3) AND ("version" >= 1)))"""
    assert influx_query == expected_influx_query


def test_iter_exprs():
    metric_filter = {
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

    expr = Expr.from_json(metric_filter)
    all_sub_exprs = list(expr)
    assert len(all_sub_exprs) == 46
    assert len([e for e in all_sub_exprs if type(e) is And]) == 2
    assert len([e for e in all_sub_exprs if type(e) is Or]) == 2
    assert len([e for e in all_sub_exprs if type(e) is Equal]) == 7
    assert len([e for e in all_sub_exprs if type(e) is NotEqual]) == 1
    assert len([e for e in all_sub_exprs if type(e) is StringLiteral]) == 3
    assert len([e for e in all_sub_exprs if type(e) is IntLiteral]) == 6
    assert len([e for e in all_sub_exprs if type(e) is FloatLiteral]) == 1
    assert len([e for e in all_sub_exprs if type(e) is DateTimeLiteral]) == 2
    assert len([e for e in all_sub_exprs if type(e) is RegexLiteral]) == 2
    assert len([e for e in all_sub_exprs if type(e) is SchemaLiteral]) == 14
    assert len([e for e in all_sub_exprs if type(e) is MatchRegex]) == 1
    assert len([e for e in all_sub_exprs if type(e) is InverseMatchRegex]) == 1
    assert len([e for e in all_sub_exprs if type(e) is GreaterThan]) == 1
    assert len([e for e in all_sub_exprs if type(e) is GreaterThanOrEqual]) == 1
    assert len([e for e in all_sub_exprs if type(e) is LessThan]) == 1
    assert len([e for e in all_sub_exprs if type(e) is LessThanOrEqual]) == 1


def test_select_stmt():
    metric_filter = {
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

    assert Select(measurement='m', retention_policy='rp', db='db',
                  columns=['a', 'b'], where=metric_filter).to_query() == \
        """SELECT "a","b" FROM "db"."rp"."m" WHERE ("act_type" = 'logging') AND ("expected_fire_rate" = 99.9) AND ("expected_fire_volume" = 10000) AND ("rule_name" != 'logging_rddms') AND ("rule_name" !~ /logging_r..s/) AND ("rule_name" =~ /logging_.*/) AND (("rule_id" = '7777') OR ("rule_id" = 6666) OR ("rule_id" = 8888)) AND (("version" = 4) OR (("create_ts" <= '2015-12-31T12:05:00Z') AND ("create_ts" > '2014-01-01T00:00:00Z') AND ("version" < 3) AND ("version" >= 1)))"""
    assert Select(measurement='m', db='db',
                  columns=[], where=metric_filter).to_query() == \
        """SELECT * FROM "db".."m" WHERE ("act_type" = 'logging') AND ("expected_fire_rate" = 99.9) AND ("expected_fire_volume" = 10000) AND ("rule_name" != 'logging_rddms') AND ("rule_name" !~ /logging_r..s/) AND ("rule_name" =~ /logging_.*/) AND (("rule_id" = '7777') OR ("rule_id" = 6666) OR ("rule_id" = 8888)) AND (("version" = 4) OR (("create_ts" <= '2015-12-31T12:05:00Z') AND ("create_ts" > '2014-01-01T00:00:00Z') AND ("version" < 3) AND ("version" >= 1)))"""
    assert Select(measurement='m').to_query() == \
        'SELECT * FROM "m"'


def test_show_tag_keys():
    metric_filter = {
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

    assert ShowTagKeys(measurement='m', retention_policy='rp', db='db', where=metric_filter).to_query() == \
        """SHOW TAG KEYS ON "db" FROM "rp"."m" WHERE ("act_type" = 'logging') AND ("expected_fire_rate" = 99.9) AND ("expected_fire_volume" = 10000) AND ("rule_name" != 'logging_rddms') AND ("rule_name" !~ /logging_r..s/) AND ("rule_name" =~ /logging_.*/) AND (("rule_id" = '7777') OR ("rule_id" = 6666) OR ("rule_id" = 8888)) AND (("version" = 4) OR (("create_ts" <= '2015-12-31T12:05:00Z') AND ("create_ts" > '2014-01-01T00:00:00Z') AND ("version" < 3) AND ("version" >= 1)))"""
    assert ShowTagKeys(measurement='m').to_query() == \
        'SHOW TAG KEYS FROM "m"'


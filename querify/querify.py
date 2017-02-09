from datetime import datetime
from typing import Union, Dict, Optional, Any, List

from .errors import InvalidQuery, UnrecognizedExprType, UnrecognizedJsonableClass


JsonDictType = Dict[str, Any]
JsonType = Union[int, float, str, JsonDictType]


class ClassWithSubclassDictMeta(type):
    def __init__(cls, what, bases=None, dict=None):
        super().__init__(what, bases, dict)
        if dict is None:
            return
        is_base = dict.get('base', False)
        if is_base:
            cls.subclasses = {}
        cls.base = is_base
        cls_key = dict.get('key')
        if cls_key is not None:
            if is_base:
                all_cls_subclasses = (getattr(bcl, 'subclasses', None) for bcl in bases)
            else:
                all_cls_subclasses = [getattr(cls, 'subclasses', None)]
            all_cls_subclasses = [c for c in all_cls_subclasses if c is not None]
            if not all_cls_subclasses:
                raise KeyError('class "{}" has key ("{}") but no base class is found'
                               .format(cls.__name__, cls_key))
            for cls_subclasses in all_cls_subclasses:
                cls_existing_subcls = cls_subclasses.get(cls_key)
                if cls_existing_subcls is not None:
                    raise KeyError('key "{}" of class "{}" conflicts with class "{}"'
                                   .format(cls_key, cls.__name__, cls_existing_subcls.__name__))
                cls_subclasses[cls_key] = cls

    def __getitem__(cls, cls_key):
        cls_subclasses = getattr(cls, 'subclasses', None)
        if cls_subclasses is None:
            raise KeyError('no base class is found for class "{}"'.format(cls.__name__))
        subcls = cls_subclasses.get(cls_key)
        if subcls is None:
            raise KeyError('no class has key "{}"'.format(cls_key))
        return subcls


class ClassFromJsonWithSubclassDictMeta(ClassWithSubclassDictMeta):
    def __init__(cls, what, bases=None, dict=None):
        super().__init__(what, bases, dict)
        for bcl in bases:
            if getattr(bcl, 'final', False):
                raise TypeError('class "{}" cannot subclass from "{}" which has been marked as final'
                                .format(cls.__name__, bcl.__name__))
        is_final = dict.get('final', False)
        cls.final = is_final

    def new_from_json(cls, json: JsonType):
        for key in cls.cls_keys_from_json(json):
            try:
                subcls = cls[key]
                if subcls.final:
                    kwargs = subcls.init_args_from_json(json)
                    if kwargs is not None:
                        return subcls(**kwargs)
                    else:
                        raise UnrecognizedJsonableClass('class "{}" is marked final but no init args are defined'
                                                        .format(subcls.__name__))
                else:
                    return subcls.new_from_json(json)
            except (KeyError, UnrecognizedExprType):
                pass
        raise UnrecognizedJsonableClass('cannot recognize class from "{}"'.format(json))

    def cls_keys_from_json(cls, json: JsonType):
        return; yield

    def init_args_from_json(cls, json) -> Optional[Dict[str, Any]]:
        pass


class Query:
    def to_query(self):
        pass


class InfluxQL(Query):
    def to_query(self) -> str:
        return ''

    def __str__(self):
        return self.to_query()


class Expr(InfluxQL, metaclass=ClassFromJsonWithSubclassDictMeta):
    base = True

    @classmethod
    def from_json(cls, json: Union['Expr', JsonType]) -> 'Expr':
        if isinstance(json, cls):
            expr = json
        else:
            try:
                expr = cls.new_from_json(json)
            except UnrecognizedJsonableClass as err:
                expr = err
        if not isinstance(expr, cls):
            raise InvalidQuery('Unexpected expression type for json {!r}. Expected "{}", but got "{}({})".'
                               .format(json, cls.__name__, type(expr).__name__, expr))
        return expr

    @classmethod
    def cls_keys_from_json(cls, json: JsonType):
        if isinstance(json, dict):
            yield 'operator_expr'
        else:
            yield 'literal'

    def __iter__(self):
        return self.iter_expr()

    def iter_expr(self):
        yield self
        for sub_expr in self.iter_sub_expr():
            yield from sub_expr.iter_expr()

    def iter_sub_expr(self):
        return; yield


class LiteralExpr(Expr):
    base = True
    key = 'literal'

    def __init__(self, literal):
        super().__init__()
        if not self.validate_literal(literal):
            raise InvalidQuery('Invalid type of literal "{}". Expected "{}", but got "{}"'
                               .format(literal, self.key.__name__, type(literal).__name__))
        self.literal = literal

    def validate_literal(self, literal):
        return isinstance(self.key, type) and isinstance(literal, self.key)

    @classmethod
    def cls_keys_from_json(cls, json):
        yield type(json)

    @classmethod
    def init_args_from_json(cls, json):
        return {'literal': json}

    def __repr__(self, *args, **kwargs):
        return '{}({!r})'.format(type(self).__name__, self.literal)


class StringLiteral(LiteralExpr):
    final = True
    key = str

    def to_query(self) -> str:
        return "'{}'".format(self.literal)


class IntLiteral(LiteralExpr):
    final = True
    key = int

    def to_query(self) -> str:
        return repr(self.literal)


class FloatLiteral(LiteralExpr):
    final = True
    key = float

    def to_query(self) -> str:
        return repr(self.literal)


class DateTimeLiteral(LiteralExpr):
    final = True
    key = datetime

    def to_query(self) -> str:
        return "'{:%Y-%m-%dT%H:%M:%SZ}'".format(self.literal)


class RegexLiteral(LiteralExpr):
    final = True
    key = 'regex'

    def validate_literal(self, literal):
        return isinstance(literal, str)

    @classmethod
    def cls_keys_from_json(cls, json):
        yield 'regex'

    def to_query(self) -> str:
        return '/{}/'.format(self.literal)


class SchemaLiteral(LiteralExpr):
    final = True
    key = 'schema'

    def validate_literal(self, literal):
        return isinstance(literal, str)

    @classmethod
    def cls_keys_from_json(cls, json):
        yield 'schema'

    def to_query(self) -> str:
        return '"{}"'.format(self.literal)


# Operator Expr
class OperatorExpr(Expr):
    base = True
    key = 'operator_expr'

    operator = ''

    @classmethod
    def normalize_eval_expr_dict(cls, filter: dict) -> dict:
        exprs = []
        for tag, tag_filter in filter.items():
            if isinstance(tag_filter, str):
                if tag_filter.startswith('/') and tag_filter.endswith('/'):
                    exprs.append({tag: {'__regex__': tag_filter[1:-1]}})
                else:
                    exprs.append({tag: {'__eq__': tag_filter}})
            elif isinstance(tag_filter, (int, float)):
                exprs.append({tag: {'__eq__': tag_filter}})
            elif isinstance(tag_filter, list):
                if tag == '__and__':
                    exprs.extend([cls.normalize_eval_expr_dict(c) for c in tag_filter])
                elif tag == '__or__':
                    exprs.append({'__or__': [cls.normalize_eval_expr_dict(c) for c in tag_filter]})
                else:
                    exprs.append({'__or__': [{tag: {'__eq__': v}} for v in tag_filter]})
            elif isinstance(tag_filter, dict):
                for op, condition in tag_filter.items():
                    if isinstance(condition, (str, int, float, datetime)):
                        exprs.append({tag: {op: condition}})
                    elif isinstance(condition, list):
                        if op == '__in__':
                            exprs.append({'__or__': [{tag: {'__eq__': v}} for v in condition]})
                        else:
                            raise InvalidQuery('"{}" operator is not applied on a list (actually got "{}").'
                                               .format(op, condition))
                    else:
                        raise InvalidQuery('Query condition is unrecognized: {!r}'
                                           .format(condition))
            else:
                raise InvalidQuery('Invalid query "{{ {}: {} }}". '
                                   'A tag\' filter must be of one of the following types: '
                                   'regex / string / numerical / list / a dict {operator: operand}.'
                                   .format(tag, tag_filter))
        if len(exprs) > 1:
            return {'__and__': exprs}
        elif len(exprs) == 1:
            return exprs[0]
        else:
            return {}

    @classmethod
    def new_from_json(cls, json: dict):
        """
        :param json:
        :return:
        :raise: UnrecognizedExprType
        """
        json = cls.normalize_eval_expr_dict(json)
        return type(cls).new_from_json(cls, json)

    @classmethod
    def cls_keys_from_json(cls, json):
        k, v = next(iter(json.items()))
        yield k
        if isinstance(v, dict) and v:
            yield next(iter(v))


# Boolean Expr
class BooleanExpr(OperatorExpr):
    pass


class BinaryBooleanExpr(BooleanExpr):
    def __init__(self, left: Union[SchemaLiteral, str], right):
        super().__init__()
        self.left = SchemaLiteral.from_json(left)
        self.right = LiteralExpr.from_json(right)

    @classmethod
    def init_args_from_json(cls, json):
        try:
            left, right_expr = next(iter(json.items()))
            op, right = next(iter(right_expr.items()))
            return {'left': left, 'right': right}
        except StopIteration:
            pass

    def iter_sub_expr(self):
        yield self.left
        yield self.right

    def to_query(self):
        return '{} {} {}'.format(self.left.to_query(), self.operator, self.right.to_query())

    def __repr__(self):
        return '{}(left={}, right={})'.format(type(self).__name__, self.left, self.right)


class Equal(BinaryBooleanExpr):
    operator = '='
    key = '__eq__'
    final = True


class NotEqual(BinaryBooleanExpr):
    operator = '!='
    key = '__neq__'
    final = True


class GreaterThan(BinaryBooleanExpr):
    operator = '>'
    key = '__gt__'
    final = True


class GreaterThanOrEqual(BinaryBooleanExpr):
    operator = '>='
    key = '__gte__'
    final = True


class LessThan(BinaryBooleanExpr):
    operator = '<'
    key = '__lt__'
    final = True


class LessThanOrEqual(BinaryBooleanExpr):
    operator = '<='
    key = '__lte__'
    final = True


class MatchRegex(BinaryBooleanExpr):
    operator = '=~'
    key = '__regex__'
    final = True

    def __init__(self, left, right):
        super().__init__(left, right)
        self.right = RegexLiteral.from_json(right)


class InverseMatchRegex(BinaryBooleanExpr):
    operator = '!~'
    key = '__iregex__'
    final = True

    def __init__(self, left, right):
        super().__init__(left, right)
        self.right = RegexLiteral.from_json(right)


class LogicalExpr(BooleanExpr):
    def __init__(self, exprs: List[Union[BooleanExpr, JsonDictType]]):
        super().__init__()
        if not isinstance(exprs, list):
            raise InvalidQuery('The "{}" operator is not applied on a list.'.format(self.operator_json))
        self.exprs = [BooleanExpr.from_json(e) for e in exprs]

    def to_query(self):
        return ' {} '.format(self.operator).join(sorted('(' + e.to_query() + ')' for e in self.exprs))

    def __repr__(self):
        return '{}({!r})'.format(type(self).__name__, self.exprs)

    @classmethod
    def init_args_from_json(cls, json):
        try:
            exprs = next(iter(json.values()))
            return {'exprs': exprs}
        except StopIteration:
            pass

    def iter_sub_expr(self):
        yield from self.exprs


class And(LogicalExpr):
    operator = 'AND'
    key = '__and__'
    final = True


class Or(LogicalExpr):
    operator = 'OR'
    key = '__or__'
    final = True


# Statement
class Stmt(InfluxQL):
    pass


class Select(Stmt):
    def __init__(self, measurement: Union[SchemaLiteral, str],
                 retention_policy: Optional[Union[SchemaLiteral, str]] = None,
                 db: Optional[Union[SchemaLiteral, str]] = None,
                 columns: Optional[List[Union[SchemaLiteral, str]]] = None,
                 where: Optional[Union[BooleanExpr, JsonDictType]] = None):
        super().__init__()
        self.measurement = SchemaLiteral.from_json(measurement)
        self.retention_policy = retention_policy and SchemaLiteral.from_json(retention_policy) or retention_policy
        self.db = db and SchemaLiteral.from_json(db) or db
        self.columns = columns and [SchemaLiteral.from_json(c) for c in columns] or columns
        self.where = where and BooleanExpr.from_json(where) or where

    def to_query(self):
        if self.columns:
            ql_columns = ','.join(c.to_query() for c in self.columns)
        else:
            ql_columns = '*'

        if self.db:
            if self.retention_policy:
                ql_db = self.db.to_query() + '.' + self.retention_policy.to_query() + '.' + self.measurement.to_query()
            else:
                ql_db = self.db.to_query() + '..' + self.measurement.to_query()
        elif self.retention_policy:
            ql_db = self.retention_policy.to_query() + '.' + self.measurement.to_query()
        else:
            ql_db = self.measurement.to_query()

        if self.where:
            ql_where = self.where.to_query()
            if ql_where:
                ql_where = ' WHERE ' + ql_where
        else:
            ql_where = ''

        return 'SELECT {} FROM {}{}'.format(ql_columns, ql_db, ql_where)


class ShowTagKeys(Stmt):
    def __init__(self, measurement: Optional[Union[SchemaLiteral, str]] = None,
                 retention_policy: Optional[Union[SchemaLiteral, str]] = None,
                 db: Optional[Union[SchemaLiteral, str]] = None,
                 where: Optional[Union[BooleanExpr, JsonDictType]] = None):
        super().__init__()
        self.measurement = measurement and SchemaLiteral.from_json(measurement) or measurement
        self.retention_policy = retention_policy and SchemaLiteral.from_json(retention_policy) or retention_policy
        self.db = db and SchemaLiteral.from_json(db) or db
        self.where = where and BooleanExpr.from_json(where) or where

    def to_query(self):
        if self.db:
            ql_on = ' ON ' + self.db.to_query()
        else:
            ql_on = ''

        if self.measurement:
            if self.retention_policy:
                ql_from = ' FROM ' + self.retention_policy.to_query() + '.' + self.measurement.to_query()
            else:
                ql_from = ' FROM ' + self.measurement.to_query()
        else:
            ql_from = ''

        if self.where:
            ql_where = self.where.to_query()
            if ql_where:
                ql_where = ' WHERE ' + ql_where
        else:
            ql_where = ''

        return 'SHOW TAG KEYS{}{}{}'.format(ql_on, ql_from, ql_where)

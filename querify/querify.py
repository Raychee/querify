import re
from datetime import datetime
from typing import Union, Dict, Optional, Any, List

from .errors import InvalidQuery, UnrecognizedExprType, UnrecognizedJsonableClass


JsonObjectType = Dict[str, Any]
JsonValueType = Union[int, float, str]
JsonType = Union[JsonValueType, JsonObjectType]


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
                elif subcls.new_from_json is cls.new_from_json:
                    raise UnrecognizedJsonableClass('class "{}" constructs an object from json "{!r}" '
                                                    'by calling itself recursively, '
                                                    'no subclass\' construct method can be found'
                                                    .format(subcls.__name__, json))
                else:
                    return subcls.new_from_json(json)
            except (KeyError, UnrecognizedExprType):
                pass
        raise UnrecognizedJsonableClass('cannot recognize class from "{}"'.format(json))

    def cls_keys_from_json(cls, json: JsonType):
        return; yield

    def init_args_from_json(cls, json) -> Optional[JsonObjectType]:
        pass


class Query:
    def to_query(self, type: str):
        method = getattr(self, 'to_query_' + type, None)
        if method is None:
            raise NotImplementedError('generating {} from {!r} is not supported'.format(type, self))
        return method()

    def to_query_influx(self) -> str:
        raise NotImplementedError('generating InfluxQL from {!r} is not implemented'.format(self))

    def to_query_mysql(self) -> str:
        raise NotImplementedError('generating MySQL from {!r} is not implemented'.format(self))

    def to_query_mongo(self) -> JsonType:
        raise NotImplementedError('generating MongoDB query from {!r} is not implemented'.format(self))

    def to_query_pandas(self) -> str:
        raise NotImplementedError('generating pandas query from {!r} is not implemented.'.format(self))

    def to_query_pluto(self) -> str:
        raise NotImplementedError('generating pluto conditions from {!r} is not implemented.'.format(self))


class Expr(Query, metaclass=ClassFromJsonWithSubclassDictMeta):
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

    def to_query_influx(self) -> str:
        return "'{}'".format(self.literal)

    def to_query_mysql(self) -> str:
        return "'{}'".format(self.literal)

    def to_query_mongo(self) -> JsonType:
        return self.literal

    def to_query_pandas(self) -> str:
        return "'{}'".format(self.literal)

    def to_query_pluto(self) -> str:
        return "\"{}\"".format(self.literal)


class BooleanLiteral(LiteralExpr):
    final = True
    key = bool

    def to_query_influx(self) -> str:
        return repr(self.literal)

    def to_query_mysql(self) -> str:
        return repr(self.literal)

    def to_query_mongo(self) -> JsonType:
        return self.literal

    def to_query_pandas(self) -> str:
        return repr(self.literal)


class IntLiteral(LiteralExpr):
    final = True
    key = int

    def to_query_influx(self) -> str:
        return repr(self.literal)

    def to_query_mysql(self) -> str:
        return repr(self.literal)

    def to_query_mongo(self) -> JsonType:
        return self.literal

    def to_query_pandas(self) -> str:
        return repr(self.literal)

    def to_query_pluto(self) -> str:
        return repr(self.literal)


class FloatLiteral(LiteralExpr):
    final = True
    key = float

    def to_query_influx(self) -> str:
        return repr(self.literal)

    def to_query_mysql(self) -> str:
        return repr(self.literal)

    def to_query_mongo(self) -> JsonType:
        return self.literal

    def to_query_pandas(self) -> str:
        return repr(self.literal)

    def to_query_pluto(self) -> str:
        return repr(self.literal)


class DateTimeLiteral(LiteralExpr):
    final = True
    key = datetime

    def to_query_influx(self) -> str:
        return "'{:%Y-%m-%dT%H:%M:%SZ}'".format(self.literal)

    def to_query_mysql(self) -> str:
        return "'{:%Y-%m-%d %H:%M:%S}'".format(self.literal)

    def to_query_mongo(self) -> JsonType:
        return self.literal


class RegexLiteral(LiteralExpr):
    final = True
    key = 'regex'

    def validate_literal(self, literal):
        return isinstance(literal, str)

    @classmethod
    def cls_keys_from_json(cls, json):
        yield 'regex'

    def to_query_influx(self) -> str:
        return '/{}/'.format(self.literal)

    def to_query_mysql(self) -> str:
        return "'{}'".format(self.literal)

    def to_query_mongo(self):
        return re.compile(self.literal)

    def to_query_pluto(self) -> str:
        return "\"{}\"".format(self.literal)


class SchemaLiteral(LiteralExpr):
    final = True
    key = 'schema'

    def validate_literal(self, literal):
        return isinstance(literal, str)

    @classmethod
    def cls_keys_from_json(cls, json):
        yield 'schema'

    def to_query_influx(self) -> str:
        return '"{}"'.format(self.literal)

    def to_query_mysql(self) -> str:
        return self.literal

    def to_query_mongo(self) -> JsonType:
        return self.literal

    def to_query_pandas(self) -> str:
        return self.literal

    def to_query_pluto(self) -> str:
        return self.literal


# Operator Expr
class OperatorExpr(Expr):
    base = True
    key = 'operator_expr'

    operator_influx = None
    operator_mysql = None
    operator_mongo = None
    operator_pandas = None
    operator_pluto = None

    @classmethod
    def normalize_eval_expr_dict(cls, filter: dict) -> dict:
        exprs = []
        for tag, tag_filter in sorted(filter.items()):
            if isinstance(tag_filter, str):
                if tag_filter.startswith('/') and tag_filter.endswith('/'):
                    exprs.append({tag: {MatchRegex.key: tag_filter[1:-1]}})
                else:
                    exprs.append({tag: {EqualValue.key: tag_filter}})
            elif isinstance(tag_filter, (int, float)):
                exprs.append({tag: {EqualValue.key: tag_filter}})
            elif isinstance(tag_filter, list):
                if tag == And.key:
                    exprs.extend(tag_filter)
                elif tag == Or.key:
                    exprs.append({Or.key: tag_filter})
                elif tag == Any.key:
                    exprs.append({Any.key: tag_filter})
                else:
                    exprs.append({tag: {In.key: tag_filter}})
            elif isinstance(tag_filter, dict):
                if tag == Not.key:
                    exprs.append({Not.key: tag_filter})
                else:
                    for op, condition in sorted(tag_filter.items()):
                        if isinstance(condition, (str, int, float, datetime)):
                            exprs.append({tag: {op: condition}})
                        elif isinstance(condition, list):
                            if op in (In.key, NotIn.key):
                                exprs.append({tag: {op: condition}})
                            else:
                                raise InvalidQuery('"{}" operator cannot be applied on a list.'
                                                   .format(op))
                        else:
                            raise InvalidQuery('Query condition is unrecognized: {!r}'
                                               .format(condition))
            else:
                raise InvalidQuery('Invalid query "{{ {}: {} }}". '
                                   'A tag\' filter must be of one of the following types: '
                                   'regex / string / numerical / list / a dict {{ operator: operand }}.'
                                   .format(tag, tag_filter))
        if len(exprs) == 1:
            return exprs[0]
        else:
            return {And.key: exprs}

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


class UnaryBooleanExpr(BooleanExpr):
    def __init__(self, operand: Union[Expr, JsonObjectType]):
        super().__init__()
        self.operand = BooleanExpr.from_json(operand)

    @classmethod
    def init_args_from_json(cls, json):
        try:
            _, operand = next(iter(json.items()))
            return {'operand': operand}
        except StopIteration:
            pass

    def iter_sub_expr(self):
        yield self.operand

    def __repr__(self):
        return '{}(operand={})'.format(type(self).__name__, self.operand)


class Not(UnaryBooleanExpr):
    final = True
    key = '__not__'
    # operator_influx = '<not>'
    operator_mysql = 'NOT'
    operator_mongo = '$not'
    operator_pandas = '~'
    operator_pluto = 'it is not true that'

    # def to_query_influx(self):
    #     return '{} {}'.format(self.operator_influx, self.operand.to_query_influx())

    def to_query_mysql(self) -> str:
        return '{} ({})'.format(self.operator_mysql, self.operand.to_query_mysql())

    def to_query_mongo(self) -> JsonType:
        tmp_mongo_query = self.operand.to_query_mongo()
        k, v = next(iter(tmp_mongo_query.items()))
        return {k: {self.operator_mongo: v}}

    def to_query_pandas(self) -> str:
        return '{}({})'.format(self.operator_pandas, self.operand.to_query_pandas())

    def to_query_pluto(self) -> str:
        return '{} {}'.format(self.operator_pluto, self.operand.to_query_pluto())


class BinaryBooleanExpr(BooleanExpr):
    def __init__(self, left, right):
        super().__init__()
        self.left = SchemaLiteral.from_json(left)
        self.right = right

    @classmethod
    def init_args_from_json(cls, json):
        try:
            left, right_expr = next(iter(json.items()))
            op, right = next(iter(right_expr.items()))
            return {'left': left, 'right': right}
        except StopIteration:
            pass

    def __repr__(self):
        return '{}(left={}, right={})'.format(type(self).__name__, self.left, self.right)


class BinaryComparisonExpr(BinaryBooleanExpr):
    def iter_sub_expr(self):
        yield self.left
        yield self.right

    def to_query_influx(self):
        if self.operator_influx is None:
            raise NotImplementedError('generating InfluxQL from operator "{}" is not implemented'.format(self.key))
        return '{} {} {}'.format(self.left.to_query_influx(), self.operator_influx, self.right.to_query_influx())

    def to_query_mysql(self) -> str:
        if self.operator_mysql is None:
            raise NotImplementedError('generating MySQL from operator "{}" is not implemented'.format(self.key))
        return '{} {} {}'.format(self.left.to_query_mysql(), self.operator_mysql, self.right.to_query_mysql())

    def to_query_mongo(self) -> JsonType:
        if self.operator_mongo is None:
            raise NotImplementedError('generating MongoDB query from operator "{}" is not implemented'.format(self.key))
        return {self.left.to_query_mongo(): {self.operator_mongo: self.right.to_query_mongo()}}

    def to_query_pandas(self) -> str:
        if self.operator_pandas is None:
            raise NotImplementedError('generating pandas query from operator "{}" is not implemented'.format(self.key))
        return '{} {} {}'.format(self.left.to_query_pandas(), self.operator_pandas, self.right.to_query_pandas())

    def to_query_pluto(self) -> str:
        if self.operator_pluto is None:
            raise NotImplementedError('generating pluto conditions from operator "{}" is not implemented'.format(self.key))
        return '{} {} {}'.format(self.left.to_query_pluto(), self.operator_pluto, self.right.to_query_pluto())


class FieldCompareValueExpr(BinaryComparisonExpr):
    def __init__(self, left: Union[SchemaLiteral, str], right):
        super().__init__(left, right)
        self.right = LiteralExpr.from_json(right)


class FieldCompareFieldExpr(BinaryComparisonExpr):
    def __init__(self, left: Union[SchemaLiteral, str], right: Union[SchemaLiteral, str]):
        super().__init__(left, right)
        if not isinstance(self.right, (SchemaLiteral, str)):
            raise InvalidQuery('The operand of "{}" must be of string type referring to a field name.'.format(self.key))
        self.right = SchemaLiteral.from_json(right)


class FieldAssertionExpr(BinaryBooleanExpr):
    def __init__(self, left: Union[SchemaLiteral, str], right: Union[BooleanLiteral, bool]):
        super().__init__(left, right)
        if not isinstance(self.right, (BooleanLiteral, bool)):
            raise InvalidQuery('The operand of "{}" must be either true or false.'.format(self.key))
        self.right = BooleanLiteral.from_json(right)


class Equal:
    operator_influx = '='
    operator_mysql = '='
    operator_pandas = '=='
    operator_pluto = 'equals'


# Inheritance order matters! WHY??
class EqualValue(Equal, FieldCompareValueExpr):
    final = True
    key = '__eq__'

    operator_mongo = '$eq'


class EqualField(Equal, FieldCompareFieldExpr):
    final = True
    key = '__eqf__'


class NotEqual:
    operator_influx = '!='
    operator_mysql = '<>'
    operator_pandas = '!='
    operator_pluto = 'does not equal'


class NotEqualValue(NotEqual, FieldCompareValueExpr):
    final = True
    key = '__neq__'

    operator_mongo = '$ne'


class NotEqualField(NotEqual, FieldCompareFieldExpr):
    final = True
    key = '__neqf__'


class GreaterThan:
    operator_influx = '>'
    operator_mysql = '>'
    operator_pandas = '>'
    operator_pluto = 'is more than'


class GreaterThanValue(GreaterThan, FieldCompareValueExpr):
    final = True
    key = '__gt__'

    operator_mongo = '$gt'


class GreaterThanField(GreaterThan, FieldCompareFieldExpr):
    final = True
    key = '__gtf__'


class GreaterThanOrEqual:
    operator_influx = '>='
    operator_mysql = '>='
    operator_pandas = '>='
    operator_pluto = 'is at least'


class GreaterThanOrEqualValue(GreaterThanOrEqual, FieldCompareValueExpr):
    final = True
    key = '__gte__'

    operator_mongo = '$gte'


class GreaterThanOrEqualField(GreaterThanOrEqual, FieldCompareFieldExpr):
    final = True
    key = '__gtef__'


class LessThan:
    operator_influx = '<'
    operator_mysql = '<'
    operator_pandas = '<'
    operator_pluto = 'is less than'


class LessThanValue(LessThan, FieldCompareValueExpr):
    final = True
    key = '__lt__'

    operator_mongo = '$lt'


class LessThanField(LessThan, FieldCompareFieldExpr):
    final = True
    key = '__ltf__'


class LessThanOrEqual:
    operator_influx = '<='
    operator_mysql = '<='
    operator_pandas = '<='
    operator_pluto = 'is at most'


class LessThanOrEqualValue(LessThanOrEqual, FieldCompareValueExpr):
    final = True
    key = '__lte__'

    operator_mongo = '$lte'


class LessThanOrEqualField(LessThanOrEqual, FieldCompareFieldExpr):
    final = True
    key = '__ltef__'


class MatchRegex(FieldCompareValueExpr):
    final = True
    key = '__regex__'
    operator_influx = '=~'
    operator_mysql = 'REGEXP'

    def __init__(self, left, right):
        super().__init__(left, right)
        self.right = RegexLiteral.from_json(right)

    def to_query_mongo(self) -> JsonType:
        return {self.left.to_query_mongo(): self.right.to_query_mongo()}


class InverseMatchRegex(FieldCompareValueExpr):
    final = True
    key = '__iregex__'
    operator_influx = '!~'
    operator_mysql = 'NOT REGEXP'

    def __init__(self, left, right):
        super().__init__(left, right)
        self.right = RegexLiteral.from_json(right)

    def to_query_mongo(self) -> JsonType:
        return {self.left.to_query_mongo(): {'$not': self.right.to_query_mongo()}}


class Null(FieldAssertionExpr):
    final = True
    key = '__null__'

    def to_query_mysql(self) -> str:
        return '{} {}'.format(self.left.to_query_mysql(), 'is NULL' if self.right.literal else 'is NOT NULL')

    def to_query_mongo(self) -> JsonType:
        return {self.left.to_query_mongo(): {'$eq' if self.right.literal else '$ne': None}}

    def to_query_pandas(self) -> str:
        return '{}pandas.isnull({})'.format('' if self.right.literal else '~', self.left.to_query_pandas())

    def to_query_pluto(self) -> str:
        return '{} {}'.format(self.left.to_query_pluto(), 'is null' if self.right.literal else 'is not null')


class Missing(FieldAssertionExpr):
    final = True
    key = '__missing__'

    def to_query_mongo(self) -> JsonType:
        return {self.left.to_query_mongo(): {'$exists': not self.right.literal}}


class FieldCompareListExpr(BinaryBooleanExpr):
    def __init__(self, left: Union[SchemaLiteral, str], right: List[Union[LiteralExpr, JsonValueType]]):
        super().__init__(left, right)
        if not isinstance(self.right, list):
            raise InvalidQuery('The operand of "{}" must be a list.'.format(self.key))
        self.right = [LiteralExpr.from_json(e) for e in right]

    def iter_sub_expr(self):
        yield self.left
        yield from self.right

    def to_query_influx(self) -> str:
        return self.equivalent_fallback_expr().to_query_influx()

    def to_query_mysql(self) -> str:
        return '{} {} ({})'.format(self.left.literal, self.operator_mysql,
                                   ', '.join(e.to_query_mysql() for e in self.right))

    def to_query_mongo(self) -> JsonType:
        return {self.left.literal: {self.operator_mongo: [e.literal for e in self.right]}}

    def to_query_pandas(self):
        return self.equivalent_fallback_expr().to_query_pandas()

    def to_query_pluto(self) -> str:
        return self.equivalent_fallback_expr().to_query_influx()

    def equivalent_fallback_expr(self):
        raise NotImplementedError('No fallback equivalent exprs for {}'.format(self))


class In(FieldCompareListExpr):
    final = True
    key = '__in__'

    operator_mysql = 'IN'
    operator_mongo = '$in'

    def equivalent_fallback_expr(self):
        return Or([EqualValue(self.left, e) for e in self.right])


class NotIn(FieldCompareListExpr):
    final = True
    key = '__nin__'

    operator_mysql = 'NOT IN'
    operator_mongo = '$nin'

    def equivalent_fallback_expr(self):
        return And([NotEqualValue(self.left, e) for e in self.right])


class LogicalExpr(BooleanExpr):
    def __init__(self, exprs: List[Union[BooleanExpr, JsonObjectType]]):
        super().__init__()
        if not isinstance(exprs, list):
            raise InvalidQuery('The "{}" operator is not applied on a list.'.format(self.key))
        exprs = (BooleanExpr.from_json(e) for e in exprs)
        self.exprs = [e for e in exprs if isinstance(e, LogicalExpr) and len(e) > 0 or not isinstance(e, LogicalExpr)]

    def to_query_influx(self):
        if self.operator_influx is None:
            raise NotImplementedError('generating InfluxQL from operator "{}" is not implemented'.format(self.key))
        return ' {} '.format(self.operator_influx).join('(' + e.to_query_influx() + ')' for e in self.exprs)

    def to_query_mysql(self):
        if self.operator_mysql is None:
            raise NotImplementedError('generating MySQL from operator "{}" is not implemented'.format(self.key))
        return ' {} '.format(self.operator_mysql).join('(' + e.to_query_mysql() + ')' for e in self.exprs)

    def to_query_mongo(self) -> JsonType:
        if self.operator_mongo is None:
            raise NotImplementedError('generating MongoDB query from operator "{}" is not implemented'.format(self.key))
        return {self.operator_mongo: [e.to_query_mongo() for e in self.exprs]}

    def to_query_pandas(self) -> str:
        if self.operator_pandas is None:
            raise NotImplementedError('generating pandas query from operator "{}" is not implemented'.format(self.key))
        return ' {} '.format(self.operator_pandas).join('(' + e.to_query_pandas() + ')' for e in self.exprs)

    def to_query_pluto(self) -> str:
        if self.operator_pluto is None:
            raise NotImplementedError('generating pluto conditions from operator "{}" is not implemented'.format(self.key))
        return ' {} '.format(self.operator_pluto).join('(' + e.to_query_pluto() + ')' for e in self.exprs)

    def __repr__(self):
        return '{}({!r})'.format(type(self).__name__, self.exprs)

    def __len__(self):
        return len(self.exprs)

    def filter(self, filter_fn, recursive=False):
        filtered_exprs = [e.filter(filter_fn, recursive) if recursive and isinstance(e, LogicalExpr) else e
                          for e in self.exprs if filter_fn(e)]
        return type(self)(filtered_exprs)

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
    final = True
    key = '__and__'
    operator_influx = 'AND'
    operator_mysql = 'AND'
    operator_mongo = '$and'
    operator_pandas = '&'
    operator_pluto = 'and'


class Or_(LogicalExpr):
    operator_influx = 'OR'
    operator_mysql = 'OR'
    operator_mongo = '$or'
    operator_pandas = '|'
    operator_pluto = 'or'


class Or(Or_):
    final = True
    key = '__or__'


class Any(Or_):
    final = True
    key = '__any__'

    def to_query_pluto(self):
        return 'any of the following conditions is true :\n' + \
               '\n'.join(sorted('      - ' + e.to_query_pluto() for e in self.exprs))


# Statement
class Stmt(Query):
    pass


class Select(Stmt):
    def __init__(self, table: Union[SchemaLiteral, str],
                 retention_policy: Optional[Union[SchemaLiteral, str]] = None,
                 db: Optional[Union[SchemaLiteral, str]] = None,
                 columns: Optional[List[Union[SchemaLiteral, str]]] = None,
                 where: Optional[Union[BooleanExpr, JsonObjectType]] = None):
        super().__init__()
        self.table = SchemaLiteral.from_json(table)
        self.retention_policy = retention_policy and SchemaLiteral.from_json(retention_policy)
        self.db = db and SchemaLiteral.from_json(db)
        self.columns = columns and [SchemaLiteral.from_json(c) for c in columns]
        self.where = where and BooleanExpr.from_json(where)

    def to_query_influx(self):
        if self.columns:
            ql_select = 'SELECT ' + ','.join(c.to_query_influx() for c in self.columns)
        else:
            ql_select = 'SELECT *'

        if self.db:
            if self.retention_policy:
                ql_db = self.db.to_query_influx() + '.' + self.retention_policy.to_query_influx() + '.' + self.table.to_query_influx()
            else:
                ql_db = self.db.to_query_influx() + '..' + self.table.to_query_influx()
        elif self.retention_policy:
            ql_db = self.retention_policy.to_query_influx() + '.' + self.table.to_query_influx()
        else:
            ql_db = self.table.to_query_influx()
        ql_from = ' FROM ' + ql_db

        if self.where:
            ql_where = self.where.to_query_influx()
            if ql_where:
                ql_where = ' WHERE ' + ql_where
        else:
            ql_where = ''

        return ql_select + ql_from + ql_where

    def to_query_mysql(self):
        if self.columns:
            ql_select = 'SELECT ' + ','.join(c.to_query_mysql() for c in self.columns)
        else:
            ql_select = 'SELECT *'

        if self.db:
            ql_db = self.db.to_query_mysql() + '.' + self.table.to_query_mysql()
        else:
            ql_db = self.table.to_query_mysql()
        ql_from = ' FROM ' + ql_db

        if self.where:
            ql_where = self.where.to_query_mysql()
            if ql_where:
                ql_where = ' WHERE ' + ql_where
        else:
            ql_where = ''

        return ql_select + ql_from + ql_where


class ShowTagKeys(Stmt):
    def __init__(self, measurement: Optional[Union[SchemaLiteral, str]] = None,
                 retention_policy: Optional[Union[SchemaLiteral, str]] = None,
                 db: Optional[Union[SchemaLiteral, str]] = None,
                 where: Optional[Union[BooleanExpr, JsonObjectType]] = None):
        super().__init__()
        self.measurement = measurement and SchemaLiteral.from_json(measurement)
        self.retention_policy = retention_policy and SchemaLiteral.from_json(retention_policy)
        self.db = db and SchemaLiteral.from_json(db)
        self.where = where and BooleanExpr.from_json(where)

    def to_query_influx(self):
        if self.db:
            ql_on = ' ON ' + self.db.to_query_influx()
        else:
            ql_on = ''

        if self.measurement:
            if self.retention_policy:
                ql_from = ' FROM ' + self.retention_policy.to_query_influx() + '.' + self.measurement.to_query_influx()
            else:
                ql_from = ' FROM ' + self.measurement.to_query_influx()
        else:
            ql_from = ''

        if self.where:
            ql_where = self.where.to_query_influx()
            if ql_where:
                ql_where = ' WHERE ' + ql_where
        else:
            ql_where = ''

        return 'SHOW TAG KEYS' + ql_on + ql_from + ql_where


class ShowColumns(Stmt):
    def __init__(self, table: Union[SchemaLiteral, str], db: Optional[Union[SchemaLiteral, str]] = None):
        super().__init__()
        self.table = SchemaLiteral.from_json(table)
        self.db = db and SchemaLiteral.from_json(db)

    def to_query_influx(self):
        if self.db:
            ql_on = ' ON ' + self.db.to_query_influx()
        else:
            ql_on = ''

        ql_from = ' FROM ' + self.table.to_query_influx()

        return 'SHOW TAG KEYS' + ql_on + ql_from

    def to_query_mysql(self):
        if self.db:
            ql_from = ' FROM ' + self.db.to_query_mysql() + '.' + self.table.to_query_mysql()
        else:
            ql_from = ' FROM ' + self.table.to_query_mysql()

        return 'SHOW COLUMNS' + ql_from

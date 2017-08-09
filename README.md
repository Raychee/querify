# Querify

A simple query generator from filter conditions in form of json.

This package aims to represent a database query with a unified form of json structure, 
so that it can be converted to MySQL / Mongo / InfluxQL / Pandas queries any time when necessary.
The json structure is very much like mongodb query objects, 
making it easy to pick up quickly for users that have relevant experience.


### Installation

```
pip install querify
```
Note: Only works for **python 3**.

### Usage
Before all it starts, let's import the `Expr` class.
```python
>>> from querify import Expr
```
To build a query expression object, simply call its `from_json` method with a json object:
```python
>>> expr = Expr.from_json({"a": {"__gt__": 5}})
```
then you can convert it to any form you want:
```python
>>> expr.to_query('mysql')
'a > 5'

>>> expr.to_query('influx')
'"a" > 5'

>>> expr.to_query('mongo')
{'a': {'$gt': 5}} 

>>> expr.to_query('pandas')
'a > 5'

>>> expr.to_query('pluto')
'a is more than 5'
```

### Supported Operators
- `__eq__`  
Assert that a variable is equal to a value. e.g. `{a: {__eq__: 5}}` (Abbreviation: `{a: 5}`)
- `__neq__`  
Assert that a variable is equal to a value. e.g. `{a: {__neq__: "some_str_value"}}`
- `__gt__`  
Assert that a variable is larger than a value. e.g. `{a: {__gt__: 5}}`
- `__gte__`  
Assert that a variable is larger than or equal to a value. e.g. `{a: {__gte__: 5}}`
- `__lt__`  
Assert that a variable is smaller than a value. e.g. `{a: {__lt__: 5}}`
- `__lte__`  
Assert that a variable is smaller than or equal to a value. e.g. `{a: {__lte__: 5}}`
- `__regex__`  
Assert that a variable matches a regular expression. e.g. `{a: {__regex__: "^[a-z0-9_-]{3,16}$"}}` (Abbreviation: `{a: "/^[a-z0-9_-]{3,16}$/"}`)
- `__iregex__`  
Assert that a variable does not match a regular expression. e.g. `{a: {__iregex__: "^[a-z0-9_-]{3,16}$"}}`
- `__null__`  
Assert that a variable is null or not. e.g. `{a: {__null__: true}}`
- `__in__`  
Assert that a variable's value equals to one of the values in a list. e.g. `{a: {__in__: ["A", "B"]}}` (Abbreviation: `{a: ["A", "B"]}`)
- `__nin__`  
Assert that a variable's value does not equal to any of the values in a list. e.g. `{a: {__nin__: ["A", "B"]}}`


- `__and__` or `__all__`  
Assert that the expression holds when all of the sub-expressions in the list are true.  
e.g. `{__and__: [{a: {__gt__: 5}}, {b: {__neq__: "B"}}]}` (Abbreviation: `{a: {__gt__: 5}, b: {__neq__: "B"}}`)
- `__or__` or `__any__`  
Assert that the expression holds if any of the sub-expressions in the list is true.  
e.g. `{__or__: [{a: {__gt__: 5}}, {b: {__neq__: "B"}}]}`


- `__not__`  
Assert that the expression holds if the sub-expressions is false.  
e.g. `{__not__: {a: {__gt__: 5}}` (Equivalence: `{a: {__lte__: 5}`)


### Advanced Usage
The code should be pretty much self-explanatory.
Please refer to the [test cases](https://github.com/Raychee/querify/blob/master/querify/test/test_querify.py) in the repo and see if something interests you. :)

BTW, welcome for any PRs that make this package more powerful!


## Authors

* **Raychee** - *Initial work* - [Querify](https://github.com/Raychee/querify)
* **Rambo** - *Operators Extension*
* **Qianwen** - *Operators Extension*


## License

This project is licensed under the MIT License - see the [LICENSE](https://github.com/Raychee/querify/blob/master/LICENSE) file for details.



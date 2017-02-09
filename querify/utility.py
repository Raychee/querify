import functools


def freeze(obj, unordered_list=False):
    @functools.cmp_to_key
    def cmp_with_types(lhs, rhs):
        try:
            return (lhs > rhs) - (lhs < rhs)
        except TypeError:
            lhs = type(lhs).__name__
            rhs = type(rhs).__name__
            return (lhs > rhs) - (lhs < rhs)

    if isinstance(obj, dict):
        return tuple(sorted(((freeze(k, unordered_list), freeze(v, unordered_list))
                            for k, v in obj.items()), key=cmp_with_types))
    elif isinstance(obj, list):
        if unordered_list:
            return tuple(sorted((freeze(i, unordered_list) for i in obj), key=cmp_with_types))
        else:
            return tuple(freeze(i, unordered_list) for i in obj)
    else:
        return obj


def deep_equal(lhs, rhs, unordered_list=False):
    return freeze(lhs, unordered_list) == freeze(rhs, unordered_list)
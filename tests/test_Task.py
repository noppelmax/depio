import unittest

from depio.Task import _get_args_dict


class TestGetArgsDict(unittest.TestCase):

    def test_get_args_dict_no_additional_args(self):
        def test_fn(a, b, c):
            pass

        result = _get_args_dict(test_fn, [1, 2, 3], {})
        self.assertEqual(result, {'a': 1, 'b': 2, 'c': 3})

    def test_get_args_dict_with_kwargs(self):
        def test_fn(a, b, c, **kwargs):
            pass

        result = _get_args_dict(test_fn, [1, 2, 3], {'d': 4, 'e': 5})
        self.assertEqual(result, {'a': 1, 'b': 2, 'c': 3, 'd': 4, 'e': 5})

    def test_get_args_dict_with_kwargs_swapped(self):
        def test_fn(a, b, c, **kwargs):
            pass

        result = _get_args_dict(test_fn, [1, 2, 3], {'d': 5, 'e': 4})
        self.assertEqual(result, {'a': 1, 'b': 2, 'c': 3, 'd': 5, 'e': 4})

    def test_get_args_dict_with_partial_args_and_kwargs(self):
        def test_fn(a, b, c, *args, **kwargs):
            pass

        result = _get_args_dict(test_fn, [1], {'b': 2, 'c': 3, 'd': 4})
        self.assertEqual(result, {'a': 1, 'b': 2, 'c': 3, 'd': 4})


if __name__ == "__main__":
    unittest.main()

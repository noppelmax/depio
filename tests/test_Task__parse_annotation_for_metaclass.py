# test_Task__parse_annotation_for_metaclass.py

import unittest
from typing import Annotated

from depio.Task import _parse_annotation_for_metaclass


class A: pass  # Dummy metaclass for testing


class TestParseAnnotationForMetaclass(unittest.TestCase):

    def test_parse_annotation_for_metaclass(self):
        """Test _parse_annotation_for_metaclass"""

        def dummy_function(inputa :Annotated[str, A]) -> None: pass

        expected_result = ['inputa']  # 'return' is the annotation name for function return type
        parsed_annotations = _parse_annotation_for_metaclass(dummy_function, {}, A)
        self.assertEqual(parsed_annotations, expected_result)

    def test_parse_annotation_for_metaclass_return(self):
        """Test _parse_annotation_for_metaclass"""

        def dummy_function() -> Annotated[str, A]: pass

        expected_result = ['return']  # 'return' is the annotation name for function return type
        parsed_annotations = _parse_annotation_for_metaclass(dummy_function, {}, A)
        self.assertEqual(parsed_annotations, expected_result)

    def test_parse_annotation_for_metaclass_no_args(self):
        """Test _parse_annotation_for_metaclass"""

        def dummy_function() -> None: pass

        parsed_annotations = _parse_annotation_for_metaclass(dummy_function, {}, A)
        self.assertEqual(parsed_annotations, [])

    def test_parse_annotation_for_metaclass_no_metaclass(self):
        """Test _parse_annotation_for_metaclass when no matching metaclass in annotation"""

        # Change metaclass in function annotation
        def dummy_function() -> Annotated[str, list]: pass

        parsed_annotations = _parse_annotation_for_metaclass(dummy_function, {}, A)
        self.assertEqual(parsed_annotations, [])

    def test_parse_annotation_for_metaclass_no_annotations(self):
        """Test _parse_annotation_for_metaclass when no function annotations"""

        def dummy_function(a, b): pass  # No type annotations

        parsed_annotations = _parse_annotation_for_metaclass(dummy_function, {}, A)
        self.assertEqual(parsed_annotations, [])


if __name__ == '__main__':
    unittest.main()

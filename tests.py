import doctest
import unittest

import import_facilities


class DocTests(unittest.TestCase):

    def test_doctests(self):
        results = doctest.testmod(import_facilities)
        self.assertEqual(results.failed, 0)


if __name__ == '__main__':
    unittest.main()

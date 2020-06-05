import doctest
import unittest

import import_teis


class DocTests(unittest.TestCase):

    def test_doctests(self):
        results = doctest.testmod(import_teis)
        self.assertEqual(results.failed, 0)


if __name__ == '__main__':
    unittest.main()

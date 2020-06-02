import doctest
import unittest

import import_tes


class DocTests(unittest.TestCase):

    def test_doctests(self):
        results = doctest.testmod(import_tes)
        self.assertEqual(results.failed, 0)


if __name__ == '__main__':
    unittest.main()

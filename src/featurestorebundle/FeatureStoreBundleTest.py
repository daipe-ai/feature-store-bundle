import unittest
from injecta.testing.services_tester import test_services
from pyfonycore.bootstrap import bootstrapped_container


class FeatureStoreBundleTest(unittest.TestCase):
    def test_init(self):
        container = bootstrapped_container.init("test")

        test_services(container)


if __name__ == "__main__":
    unittest.main()

import unittest
import injecta.testing.services_tester as service_tester
from pyfonycore.bootstrap import bootstrapped_container


class FeatureStoreBundleTest(unittest.TestCase):
    def test_init(self):
        container = bootstrapped_container.init("test")

        service_tester.test_services(container)


if __name__ == "__main__":
    unittest.main()

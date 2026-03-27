import random
import unittest

from tablestore.utils import VectorUtils


def generate_random_floats(num_floats, lower_bound, upper_bound):
    """Generate a list of random floating-point numbers within a specified range and quantity."""
    return [random.uniform(lower_bound, upper_bound) for _ in range(num_floats)]


class VectorUtilsTest(unittest.TestCase):
    def test_floats_to_bytearray(self):
        # Test case 1: normal case
        floats = [1.0, 2.0, 3.0]
        bytes_list = VectorUtils.floats_to_bytes(floats)
        self.assertEqual(bytearray(b'\x00\x00\x80?\x00\x00\x00@\x00\x00@@'), bytes_list)
        # Test case 2: empty list
        floats = []
        with self.assertRaisesRegex(ValueError, "vector is empty"):
            VectorUtils.floats_to_bytes(floats)
        # Test case 3: non-float list
        floats = [1, 2, 3]
        with self.assertRaisesRegex(TypeError, "Input must be a list/tuple of floats"):
            VectorUtils.floats_to_bytes(floats)
        # Test case 4: non-list or tuple
        floats = 1.0
        with self.assertRaisesRegex(TypeError, "Input must be a list/tuple of floats"):
            VectorUtils.floats_to_bytes(floats)
        # Test case 5: tuple
        floats = (1.0, 2.0, 3.0)
        bytes_list = VectorUtils.floats_to_bytes(floats)
        self.assertEqual(bytearray(b'\x00\x00\x80?\x00\x00\x00@\x00\x00@@'), bytes_list)
        # Test case 6: non-float tuple
        floats = (1, 2, 3)
        with self.assertRaisesRegex(TypeError, "Input must be a list/tuple of floats"):
            VectorUtils.floats_to_bytes(floats)
        # Test case 7: complex floats
        floats = [1.1, 2.22, 3.333, 4.4444]
        bytes_list = VectorUtils.floats_to_bytes(floats)
        self.assertEqual(
            bytearray(i % 256 for i in [-51, -52, -116, 63, 123, 20, 14, 64, -33, 79, 85, 64, -122, 56, -114, 64]),
            bytes_list)

    def test_bytes_to_floats(self):
        # Test case 1: normal case
        bytes_list = bytearray(b'\x00\x00\x80?\x00\x00\x00@\x00\x00@@')
        floats = VectorUtils.bytes_to_floats(bytes_list)
        self.assertEqual([1.0, 2.0, 3.0], floats)
        # Test case 2: not bytearray
        bytes_list = b''
        with self.assertRaisesRegex(TypeError, "Input must be a bytearray object"):
            VectorUtils.bytes_to_floats(bytes_list)
        # Test case 3: invalid bytearray
        bytes_list = bytearray(b'\x00\x00\x80?\x00\x00\x00@\x00@@')
        with self.assertRaisesRegex(ValueError,
                                    "bytes length is not multiple of 4\\(SIZE_OF_FLOAT32\\) or length is 0"):
            VectorUtils.bytes_to_floats(bytes_list)
        # Test case 4: empty bytearray
        bytes_list = bytearray(b'')
        with self.assertRaisesRegex(ValueError,
                                    "bytes length is not multiple of 4\\(SIZE_OF_FLOAT32\\) or length is 0"):
            VectorUtils.bytes_to_floats(bytes_list)

    def test_bytes_and_floats_conversion(self):
        floats = generate_random_floats(random.randint(100, 1024), 0, 1)
        bytes_list = VectorUtils.floats_to_bytes(floats)
        floats_2 = VectorUtils.bytes_to_floats(bytes_list)
        self.assertEqual(len(floats), len(floats_2))
        for i in range(len(floats)):
            # floats_2[i] may not equal to floats[i] due to precision loss
            self.assertAlmostEqual(floats[i], floats_2[i], places=7)


if __name__ == '__main__':
    unittest.main()

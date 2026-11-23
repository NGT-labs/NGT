import tempfile
import unittest
from pathlib import Path

import ngtpy
import numpy as np


class NgtpyObjectTest(unittest.TestCase):
    def test_get_object_float16_after_save_and_reopen(self):
        # All values are exactly representable in Float16.
        values = [0.5, -1.25, 2.0, 16.0, 0.0, 0.00006103515625]

        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "index")
            ngtpy.create(path, len(values), object_type="Float16")
            index = ngtpy.Index(path)
            object_id = index.insert(values)
            index.save()
            index.close()

            index = ngtpy.Index(path)
            try:
                self.assertEqual(index.get_object(object_id), values)
            finally:
                index.close()


class NgtpyBatchSearchTest(unittest.TestCase):
    def test_numpy_views_match_contiguous_queries(self):
        objects = np.array([[0, 0], [100, 0], [0, 100], [100, 100]], dtype=np.float32)
        sliced = np.array([[1, 999, 2, 999], [99, 999, 1, 999]], dtype=np.float32)[:, ::2]
        fortran = np.asfortranarray([[1, 2], [99, 1]], dtype=np.float32)

        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "index")
            ngtpy.create(path, 2)
            index = ngtpy.Index(path)
            try:
                index.batch_insert(objects, num_threads=1)

                def search_ids(queries):
                    results = ngtpy.BatchResults()
                    index.batch_search(queries, results, size=1)
                    return [results.get(i)[0][0] for i in range(len(queries))]

                expected = search_ids(np.ascontiguousarray(sliced))
                self.assertEqual(expected, [0, 1])
                self.assertEqual(search_ids(sliced), expected)
                self.assertEqual(search_ids(fortran), expected)
            finally:
                index.close()

    def test_batch_search_rejects_invalid_shape(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "index")
            ngtpy.create(path, 2)
            index = ngtpy.Index(path)
            try:
                for queries in (np.array([1, 2], dtype=np.float32),
                                np.zeros((1, 1, 2), dtype=np.float32)):
                    with self.subTest(shape=queries.shape):
                        with self.assertRaisesRegex(RuntimeError, "two-dimensional"):
                            index.batch_search(queries, ngtpy.BatchResults(), size=1)
                with self.assertRaisesRegex(RuntimeError, "dimension"):
                    index.batch_search(np.zeros((1, 3), dtype=np.float32),
                                       ngtpy.BatchResults(), size=1)
            finally:
                index.close()


if __name__ == "__main__":
    unittest.main()

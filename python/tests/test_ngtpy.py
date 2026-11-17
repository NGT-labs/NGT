import tempfile
import unittest
from pathlib import Path

import ngtpy


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


if __name__ == "__main__":
    unittest.main()

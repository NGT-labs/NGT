import json
import subprocess
import sys
import tempfile
import textwrap
import unittest


class ThreadedSearchTest(unittest.TestCase):
    def run_python(self, code, directory, *args):
        try:
            result = subprocess.run(
                [sys.executable, "-c", textwrap.dedent(code), *args],
                cwd=directory,
                capture_output=True,
                text=True,
                timeout=15,
            )
        except subprocess.TimeoutExpired as error:
            self.fail(f"Search subprocess timed out: {error.stdout!r} {error.stderr!r}")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return result.stdout

    def test_concurrent_first_search(self):
        for mode in ("empty", "warmup", "populated"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                if mode == "populated":
                    self.run_python(
                        """
                        import ngtpy
                        ngtpy.create("index", 10)
                        index = ngtpy.Index("index")
                        index.batch_insert([[0.0] * 10, [1.0] * 10], num_threads=1)
                        index.save()
                        """,
                        directory,
                    )
                output = self.run_python(
                    """
                    from concurrent.futures import ThreadPoolExecutor
                    from threading import Barrier
                    import json
                    import sys
                    import ngtpy

                    mode = sys.argv[1]
                    query = [0.0] * 10
                    if mode != "populated":
                        ngtpy.create("index", 10)
                    index = ngtpy.Index("index")
                    if mode == "warmup":
                        index.search(query, 1)
                    ready = Barrier(2)

                    def search():
                        ready.wait()
                        return index.search(query, 1)

                    with ThreadPoolExecutor(max_workers=2) as executor:
                        futures = [executor.submit(search) for _ in range(2)]
                        print(json.dumps([future.result() for future in futures]))
                    """,
                    directory,
                    mode,
                )
                expected = [[[0, 0.0]], [[0, 0.0]]] if mode == "populated" else [[], []]
                self.assertEqual(json.loads(output), expected)


if __name__ == "__main__":
    unittest.main()

import os
import shutil
import tempfile
import unittest

from aind_data_transfer.util.file_utils import collect_filepaths, batch_files_by_size


def _create_dir_tree_with_sizes(base_dir, structure):
    for key, value in structure.items():
        if isinstance(value, dict):  # directory
            new_dir = os.path.join(base_dir, key)
            os.makedirs(new_dir, exist_ok=False)
            _create_dir_tree_with_sizes(new_dir, value)
        else:  # file
            file_path = os.path.join(base_dir, key)
            with open(file_path, 'w') as file:
                file.write('0' * value)  # creating a file of the specified size


class TestFileUtils(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

        # Complex directory structure and files with their sizes
        dir_tree = {
            'dir1': {
                'file1.txt': 100,
                'file2.tiff': 150,
                'sub_dir1': {
                    'file3.h5': 80,
                    'file4.ims': 120,
                    'sub_sub_dir1': {
                        'file5.txt': 50,
                        'file6.tiff': 70
                    }
                }
            },
            'dir2': {
                'file7.ims': 130,
                'file8.txt': 60
            },
            'exclude_dir': {
                'file9.h5': 110
            }
        }

        _create_dir_tree_with_sizes(self.test_dir, dir_tree)

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_collect_filepaths_complex(self):
        # Testing basic recursive behavior
        result = sorted(list(collect_filepaths(self.test_dir, recursive=True)))
        expected_result = [
            "dir1/file1.txt",
            "dir1/file2.tiff",
            "dir1/sub_dir1/file3.h5",
            "dir1/sub_dir1/file4.ims",
            "dir1/sub_dir1/sub_sub_dir1/file5.txt",
            "dir1/sub_dir1/sub_sub_dir1/file6.tiff",
            "dir2/file7.ims",
            "dir2/file8.txt",
            "exclude_dir/file9.h5"
        ]
        expected_result = [os.path.join(self.test_dir, path) for path in expected_result]
        self.assertEqual(result, expected_result)

        # Testing with excluded directory and included extensions
        result = sorted(list(collect_filepaths(self.test_dir, recursive=True, include_exts=['.tiff', '.h5'],
                                               exclude_dirs=['sub_sub_dir1'])))
        expected_result = [
            "dir1/file2.tiff",
            "dir1/sub_dir1/file3.h5",
            "exclude_dir/file9.h5"
        ]
        expected_result = [os.path.join(self.test_dir, path) for path in expected_result]
        self.assertEqual(result, expected_result)

        # Testing return size
        result_with_size = sorted(list(collect_filepaths(self.test_dir, recursive=True, return_size=True)))
        expected_result_with_size = [
            ("dir1/file1.txt", 100),
            ("dir1/file2.tiff", 150),
            ("dir1/sub_dir1/file3.h5", 80),
            ("dir1/sub_dir1/file4.ims", 120),
            ("dir1/sub_dir1/sub_sub_dir1/file5.txt", 50),
            ("dir1/sub_dir1/sub_sub_dir1/file6.tiff", 70),
            ("dir2/file7.ims", 130),
            ("dir2/file8.txt", 60),
            ("exclude_dir/file9.h5", 110)
        ]
        expected_result_with_size = [(os.path.join(self.test_dir, path), size) for path, size in expected_result_with_size]
        self.assertEqual(result_with_size, expected_result_with_size)

    def test_batch_files_by_size_complex(self):
        expected_files = [
            "dir1/file1.txt",
            "dir1/file2.tiff",
            "dir1/sub_dir1/file3.h5",
            "dir1/sub_dir1/file4.ims",
            "dir1/sub_dir1/sub_sub_dir1/file5.txt",
            "dir1/sub_dir1/sub_sub_dir1/file6.tiff",
            "dir2/file7.ims",
            "dir2/file8.txt",
            "exclude_dir/file9.h5"
        ]
        expected_files = [os.path.join(self.test_dir, path) for path in expected_files]

        batches = list(batch_files_by_size(self.test_dir, target_size=250))
        self._validate_batch_invariants(expected_files, batches, 250)

        batches = list(batch_files_by_size(self.test_dir, target_size=100))
        self._validate_batch_invariants(expected_files, batches, 100)

        batches = list(batch_files_by_size(self.test_dir, target_size=1000))
        self._validate_batch_invariants(expected_files, batches, 1000)

    def _validate_batch_invariants(self, expected_files, batches, target_size):
        all_files_in_batches = [file for batch in batches for file in batch]
        self.assertEqual(set(all_files_in_batches), set(expected_files))  # All files are present

        for batch in batches:
            batch_size = sum(os.path.getsize(file) for file in batch)
            self.assertTrue(len(batch) > 0)
            if len(batch) > 1:
                self.assertLessEqual(batch_size, target_size)


if __name__ == '__main__':
    unittest.main()

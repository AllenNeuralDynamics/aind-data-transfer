import unittest
from unittest.mock import patch

from aind_data_transfer.util.file_utils import collect_filepaths, batch_files_by_size


class TestFileUtils(unittest.TestCase):

    def setUp(self):
        self.mock_files = [
            ('/root', ['subdir1', 'subdir2'], ['file1.txt', 'file2.tiff']),
            ('/root/subdir1', [], ['file3.h5']),
            ('/root/subdir2', [], ['file4.ims', 'file5.txt'])
        ]

        self.mock_file_sizes = {
            '/root/file1.txt': 5,
            '/root/file2.tiff': 10,
            '/root/subdir1/file3.h5': 50,
            '/root/subdir2/file4.ims': 40,
            '/root/subdir2/file5.txt': 20,
        }

    def test_collect_filepaths(self):
        with patch('os.walk') as mock_walk:
            mock_walk.return_value = self.mock_files

            # Test without any filters
            paths = list(collect_filepaths('/root'))
            expected_paths = [
                '/root/file1.txt',
                '/root/file2.tiff',
                '/root/subdir1/file3.h5',
                '/root/subdir2/file4.ims',
                '/root/subdir2/file5.txt'
            ]
            self.assertEqual(paths, expected_paths)

            # Test with include_exts filter
            paths = list(collect_filepaths('/root', include_exts=['.tiff', '.h5']))
            expected_paths = ['/root/file2.tiff', '/root/subdir1/file3.h5']
            self.assertEqual(paths, expected_paths)

            # Test with exclude_dirs filter
            paths = list(collect_filepaths('/root', exclude_dirs=['subdir2']))
            expected_paths = ['/root/file1.txt', '/root/file2.tiff', '/root/subdir1/file3.h5']
            self.assertEqual(paths, expected_paths)

            # Test non-recursive
            paths = list(collect_filepaths('/root', recursive=False))
            expected_paths = ['/root/file1.txt', '/root/file2.tiff']
            self.assertEqual(paths, expected_paths)

    def test_batch_files_by_size(self):
        with patch('os.path.getsize', side_effect=lambda x: self.mock_file_sizes[x]):
            filepaths = [
                '/root/file1.txt',
                '/root/file2.tiff',
                '/root/subdir1/file3.h5',
                '/root/subdir2/file4.ims',
                '/root/subdir2/file5.txt',
            ]

            # Test batching by size
            batches = list(batch_files_by_size(filepaths, target_size=10))
            expected_batches = [['/root/file1.txt'], ['/root/file2.tiff'],
                                ['/root/subdir1/file3.h5'], ['/root/subdir2/file4.ims'], ['/root/subdir2/file5.txt']]
            self.assertEqual(batches, expected_batches)

            # Increase target size to group files
            batches = list(batch_files_by_size(filepaths, target_size=100))
            expected_batches = [['/root/file1.txt', '/root/file2.tiff', '/root/subdir1/file3.h5'],
                                ['/root/subdir2/file4.ims', '/root/subdir2/file5.txt']]
            self.assertEqual(batches, expected_batches)

            # Test with one large file exceeding the target size
            batches = list(batch_files_by_size(filepaths, target_size=45))
            expected_batches = [['/root/file1.txt', '/root/file2.tiff'],
                                ['/root/subdir1/file3.h5'], ['/root/subdir2/file4.ims'], ['/root/subdir2/file5.txt']]
            self.assertEqual(batches, expected_batches)

            # Test with multiple files just fitting the target size
            batches = list(batch_files_by_size(filepaths, target_size=60))
            expected_batches = [['/root/file1.txt', '/root/file2.tiff'], ['/root/subdir1/file3.h5'],
                                ['/root/subdir2/file4.ims', '/root/subdir2/file5.txt']]
            self.assertEqual(batches, expected_batches)


if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)

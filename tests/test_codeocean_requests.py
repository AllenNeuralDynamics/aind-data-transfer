import unittest
from unittest import mock
from transfer.codeocean import CodeOceanDataAssetRequests


class TestCodeOceanDataAssetRequests(unittest.TestCase):
    expected_json_data = (
        {'name': 'ecephys_625463_2022-10-06_10-14-25',
         'description': '',
         'mount': 'ecephys_625463_2022-10-06_10-14-25',
         'tags': ['ecephys'],
         'source': {
             'aws': {'bucket': 'aind-test-bucket',
                     'prefix': 'ecephys_625463_2022-10-06_10-14-25',
                     'keep_on_external_storage': True,
                     'index_data': True,
                     'access_key_id': 'AWS_ACCESS_KEY',
                     'secret_access_key': 'AWS_SECRET_ACCESS_KEY'}}
         })

    expected_request_response = (
        {
            "created": 1641420832,
            "description": expected_json_data['description'],
            "files": 0,
            "id": "44ec16c3-cb5a-45f5-93d1-cba8be800c24",
            "lastUsed": 0,
            "name": expected_json_data['name'],
            "sizeInBytes": 0,
            "state": "DATA_ASSET_STATE_DRAFT",
            "tags": expected_json_data['tags'],
            "type": "DATA_ASSET_TYPE_DATASET"
        })

    def mocked_request_post(self, *args, **kwargs):
        class MockResponse:
            def __init__(self, json_data, status_code):
                self.json_data = json_data
                self.status_code = status_code
        success_response = (
            TestCodeOceanDataAssetRequests.expected_request_response)
        return MockResponse(success_response, 200)

    def test_json_data(self):
        created_json_data = CodeOceanDataAssetRequests.create_json_data(
            asset_name='ecephys_625463_2022-10-06_10-14-25',
            mount='ecephys_625463_2022-10-06_10-14-25',
            bucket='aind-test-bucket',
            prefix='ecephys_625463_2022-10-06_10-14-25',
            access_key_id='AWS_ACCESS_KEY',
            secret_access_key='AWS_SECRET_ACCESS_KEY',
            tags=['ecephys']
        )

        self.assertEqual(self.expected_json_data, created_json_data)

    @mock.patch('requests.post', side_effect=mocked_request_post)
    def test_register_data_asset(self, _):
        url = 'https://acmecorp.codeocean.com/api/v1/data_assets'
        auth_token = 'CODEOCEAN_API_TOKEN'
        response = CodeOceanDataAssetRequests.register_data_asset(
            url=url,
            json_data=self.expected_json_data,
            auth_token=auth_token)
        self.assertEqual(200, response.status_code)
        success_response = self.expected_request_response
        self.assertEqual(success_response, response.json_data)


if __name__ == "__main__":
    unittest.main()

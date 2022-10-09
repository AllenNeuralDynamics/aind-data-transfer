import requests


class CodeOceanDataAssetRequests:

    @staticmethod
    def create_json_data(asset_name,
                         mount,
                         bucket,
                         prefix,
                         access_key_id,
                         secret_access_key,
                         tags=None,
                         asset_description='',
                         keep_on_external_storage=True,
                         index_data=True):
        tags_to_attach = [] if tags is None else tags
        json_data = {
            'name': asset_name,
            'description': asset_description,
            'mount': mount,
            'tags': tags_to_attach,
            'source': {
                'aws': {
                    'bucket': bucket,
                    'prefix': prefix,
                    'keep_on_external_storage': keep_on_external_storage,
                    'index_data': index_data,
                    'access_key_id': access_key_id,
                    'secret_access_key': secret_access_key,
                },
            },
        }
        return json_data

    @staticmethod
    def register_data_asset(url, json_data, auth_token):
        response = requests.post(url, json=json_data, auth=auth_token)
        return response

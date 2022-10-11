"""Module to interface with codeocean api.
TODO: Move this to its own project?
"""
import requests


class CodeOceanClient:
    """Client that will connect to CodeOcean"""

    def __init__(self, domain, token):
        """
        Args:
            domain (str): Domain of VPC
            token (str): Authorization token
        """
        self.domain = domain
        self.token = token


class CodeOceanDataAssetRequests(CodeOceanClient):
    """This class will handle the methods needed to manage data assets stored
    on Code Ocean's platform.
    """

    asset_url = "/api/v1/data_assets"

    @staticmethod
    def create_post_json_data(
        asset_name,
        mount,
        bucket,
        prefix,
        access_key_id,
        secret_access_key,
        tags=None,
        asset_description="",
        keep_on_external_storage=True,
        index_data=True,
    ):
        """
        This will create a json object that will be attached to a post request.
        Args:
            asset_name (str): Name of the asset
            mount (str): Mount point
            bucket (str): Currently only aws buckets are allowed
            prefix (str): The object prefix in the bucket
            access_key_id (str): AWS access key
            secret_access_key (str): AWS secret access key
            tags (list[str]): A list of tags to attach to the data asset
            asset_description (str): A description of the data asset.
              Defaults to blank.
            keep_on_external_storage (bool): Keep data asset on external
              storage. Defaults to True.
            index_data (bool): Whether to index the data asset.
              Defaults to True.

        Returns:
            A json object as documented by CodeOcean's v1 api docs.
        """
        tags_to_attach = [] if tags is None else tags
        json_data = {
            "name": asset_name,
            "description": asset_description,
            "mount": mount,
            "tags": tags_to_attach,
            "source": {
                "aws": {
                    "bucket": bucket,
                    "prefix": prefix,
                    "keep_on_external_storage": keep_on_external_storage,
                    "index_data": index_data,
                    "access_key_id": access_key_id,
                    "secret_access_key": secret_access_key,
                },
            },
        }
        return json_data

    def register_data_asset(self, json_data: dict):
        """
        Posts a request. TODO: Pass in all params instead of pre-created json?
        Args:
            json_data (json): JSON object to send as data

        Returns:
            A response (msg: json, status_code: int)
        """
        url = self.domain + self.asset_url
        response = requests.post(url, json=json_data, auth=(self.token, ""))
        return response

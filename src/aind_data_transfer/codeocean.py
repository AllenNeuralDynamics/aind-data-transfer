"""Module to interface with codeocean api.
TODO: Move this to its own project?
"""
import requests
from typing import List, Dict, Optional


class CodeOceanClient:
    """Client that will connect to CodeOcean"""

    def __init__(self,
                 domain: str,
                 token: str,
                 api_version: int = 1) -> None:
        """
        Base client for CodeOcean's API
        Parameters
        ----------
        domain : str
          VPC domain
        token : str
          API token
        api_version : int
          CodeOcean API version
        """
        self.domain = domain
        self.token = token
        self.api_version = api_version
        self.asset_url = f"{self.domain}/api/v{self.api_version}/data_assets"
        self.capsule_url = f"{self.domain}/api/v{self.api_version}/capsules"
        self.computation_url = (
            f"{self.domain}/api/v{self.api_version}/computations")


class CodeOceanDataAssetRequests(CodeOceanClient):
    """This class will handle the methods needed to manage data assets stored
    on Code Ocean's platform.
    """

    def register_data_asset(self,
                            asset_name: str,
                            mount: str,
                            bucket: str,
                            prefix: str,
                            access_key_id: Optional[str] = None,
                            secret_access_key: Optional[str] = None,
                            tags: Optional[List[str]] = None,
                            asset_description: Optional[str] = "",
                            keep_on_external_storage: Optional[bool] = True,
                            index_data: Optional[bool] = True
                            ) -> dict:
        """
        Parameters
        ---------------
        asset_name : string
            Name of the asset
        mount : string
            Mount point
        bucket : string
            Bucket name. Currently only aws buckets are allowed.
        prefix : string
            The object prefix in the bucket
        access_key_id : Optional[str]
            AWS access key. It's not necessary for public buckets.
            Default None (not provided).
        secret_access_key : Optional[str]
            AWS secret access key. It's not necessary for public buckets.
            Default None (not provided).
        tags : Optional[List[str]]
            A list of tags to attach to the data asset.
            Default None (empty list).
        asset_description : Optional[str]
            A description of the data asset. Default blanks.
        keep_on_external_storage : Optional[bool]
            Keep data asset on external storage. Defaults to True.
        index_data : Optional[bool]
            Whether to index the data asset. Defaults to True.
        Returns
        ---------------
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
                    "index_data": index_data
                },
            },
        }

        if access_key_id and secret_access_key:
            json_data['source']['aws']['access_key_id'] = access_key_id
            json_data['source']['aws']['secret_access_key'] = secret_access_key

        response = requests.post(self.asset_url,
                                 json=json_data,
                                 auth=(self.token, ""))
        return response.json()


class CodeOceanCapsuleRequests(CodeOceanClient):
    """This class will handle the methods needed to manage capsules stored
    on Code Ocean's platform.
    """

    def run_capsule(
            self,
            capsule_id: str,
            data_assets: List[Dict],
            parameters: Optional[List] = None,
    ) -> dict:
        """
        This will run a capsule/pipeline using a POST request to code ocean
        API.

        Parameters
        ---------------
        capsule_id : string
            ID of the capsule
        data_assets : List[dict]
            List of dictionaries containing the following keys: 'id' which
            refers to the data asset id in Code Ocean and 'mount' which
            refers to the data asset mount folder.
        parameters : List
            Parameters given to the capsule. Default None which means
            the capsule runs with no parameters.

            The parameters should match in order to the parameters given in the
            capsule.

            e.g.
            'parameters': [
                'input_folder',
                'output_folder',
                'bucket_name'
            ]

            where position one refers to the parameter #1 ('input_folder'),
            parameter #2 ('output_folder'), and parameter #3 ('bucket_name')

        Returns
        ---------------
        A json object as documented by CodeOcean's v1 api docs.
        """

        data = {
            'capsule_id': capsule_id,
            'data_assets': data_assets
        }

        if parameters:
            data['parameters'] = parameters

        response = requests.post(
            url=self.computation_url,
            json=data,
            auth=(self.token, "")
        )

        return response.json()

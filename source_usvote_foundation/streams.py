#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import logging
import requests
from airbyte_cdk.sources.streams.http import HttpStream

# Basic full refresh stream
class USVoteFoundationStream(HttpStream, ABC):
    """
    Parent class extended by all stream-specific classes
    """

    url_base = "https://api.usvotefoundation.org/eod/v3/"

    def __init__(
        self, config, **kwargs
    ):
        super().__init__(**kwargs)
        self.config = config
        self.url_base = "https://api.usvotefoundation.org/eod/v3/"

        if "state_id" in self.config:
            self.config["state"] = self.config["state_id"]

        if "office_id" in self.config:
            self.config["office"] = self.config["office_id"]

    def request_headers(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> Mapping[str, Any]:

        return {
            "Authorization": "OAuth " + self.config["api_key"]
        }

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        '''
        Returns the "next" URL for pagination
        '''

        response_json = response.json()

        if response_json["meta"]["next"]:
            next_url = response_json["meta"]["next"]
            next_endpoint = next_url.replace(self.url_base,"")
            return next_endpoint

    def parse_response(
        self,
        response: requests.Response,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
        **kwargs,
    ) -> Iterable[Mapping]:

        response_json = response.json()
        yield from response_json.get("objects", [])  # USVoteFoundation puts records in a container array "data"


class States(USVoteFoundationStream):
    """
    Retrieve all states.
    states endpoint: https://api.usvotefoundation.org/eod/v3/states/
    """

    primary_key = "id"

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
        **kwargs,
    ) -> str:

        if next_page_token:
            return next_page_token
        
        else:
            return "states"


class Regions(USVoteFoundationStream):
    """
    Retrieve all regions or specific regions by state.

    regions endpoint: https://api.usvotefoundation.org/eod/v3/states/
    """

    primary_key = "id"

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:

        # Don't pass query params if paginating using a "next" URL
        if next_page_token:
            return {}

        # Include only query params relevant to the regions endpoint
        region_query_param_keys = [
        "state",
        "state_abbr",
        "county",
        "county_name",
        "municipality",
        "municipality_name",
        "municipality_type",
        "region_name"
        ]

        region_query_params = {key: self.config[key] for key in region_query_param_keys if key in self.config}
        
        return region_query_params

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
        **kwargs,
    ) -> str:

        if next_page_token:
            return next_page_token
        
        else:
            return "regions"

class Offices(USVoteFoundationStream):
    """
    Retrieve all regions or specific regions by state.

    offices endpoint: https://api.usvotefoundation.org/eod/v3/offices/
    """

    primary_key = "id"

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:

        # Don't pass query params if paginating using a "next" URL
        if next_page_token:
            return {}

        # Include only query params relevant to the regions endpoint
        region_query_param_keys = [
        "region_id"
        ]

        region_query_params = {key: self.config[key] for key in region_query_param_keys if key in self.config}
        
        return region_query_params

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
        **kwargs,
    ) -> str:

        if next_page_token:
            return next_page_token
        
        else:
            return "offices"

class Officials(USVoteFoundationStream):
    """
    Retrieve all officials or specific officials by office and/or office type.

    https://api.usvotefoundation.org/eod/v3/officials[?office=<office_id>][&office_type=<office_type>]

    """

    primary_key = "id"

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:

        # Don't pass query params if paginating using a "next" URL
        if next_page_token:
            return {}

        region_query_param_keys = [
        "office_id",
        "office_type"
        ]

        region_query_params = {key: self.config[key] for key in region_query_param_keys if key in self.config}
        
        return region_query_params

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
        **kwargs,
    ) -> str:

        if next_page_token:
            return next_page_token
        
        else:
            return "officials"

'''
class TweetMedia(USVoteFoundationStream):
    """
    Fetch metrics for recent tweets created by the specified username.
    states endpoint: https://api.usvotefoundation.org/eod/v3/states/
    """

    primary_key = "id"

    def parse_response(
        self,
        response: requests.Response,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
        **kwargs,
    ) -> Iterable[Mapping]:

        response_json = response.json()

        yield response_json['objects']

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
        **kwargs,
    ) -> str:


        endpoint = (f"users/{user_id}/tweets" +
                    f"?expansions=attachments.media_keys" +
                    f"&media.fields={media_fields}"
                    )

        return endpoint


class TweetPlaces(USVoteFoundationStream):
    """
    Fetch metrics for recent tweets created by the specified username.
    2/tweets endpoint: https://developer.USVoteFoundation.com/en/docs/USVoteFoundation-api/tweets/lookup/api-reference/get-tweets
    Tweet object model: https://developer.USVoteFoundation.com/en/docs/twitt er-api/data-dictionary/object-model/tweet
    """

    primary_key = "id"
    cursor_field = "id"

    def parse_response(
        self,
        response: requests.Response,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
        **kwargs,
    ) -> Iterable[Mapping]:

        response_json = response.json()
        
        if 'includes' in response_json:
            yield response_json['includes']['places'][0]

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
        **kwargs,
    ) -> str:


        user_id = self._get_user_id()
        place_fields = self._get_place_fields()
        endpoint = (f"users/{user_id}/tweets" +
                    f"?expansions=geo.place_id" +
                    f"&place.fields={place_fields}"
                    )

        return endpoint

class UserMetrics(USVoteFoundationStream):
    """
    Returns metrics for a user identified by their username.
    """

    primary_key = "id"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def request_params(
            self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return None

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
        **kwargs,
    ) -> str:

        user_id = self._get_user_id()
        user_metric_fields = self.get_user_metric_fields()
        endpoint = (f"users?ids={user_id}"+
                    f"&user.fields={user_metric_fields}")

        return endpoint
'''
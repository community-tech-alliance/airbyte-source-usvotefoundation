#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
import logging
import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from source_USVoteFoundation.streams import (
    States,
    Regions,
    Offices,
    Officials
)

# Initialize logger and pull in loglevel from env or default to INFO
logger = logging.getLogger()
logger.setLevel("INFO")

# Source
class SourceUSVoteFoundation(AbstractSource):

    def check_connection(self, logger, config: Mapping[str, Any]) -> Tuple[bool, any]:
        """
        :param config:  the user-input config object conforming to the connector's spec.json
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """

        try:

            headers = {'Authorization': "OAuth " + config["api_key"]}
            url = "https://api.usvotefoundation.org/eod/v3/states/2"
            r = requests.get(
                        url = url,
                        headers = headers
                        )

            assert r.status_code == 200

            return True, None

        except Exception as e:

            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """

        return [
            States(config=config),
            Regions(config=config),
            Offices(config=config),
            Officials(config=config)
        ]

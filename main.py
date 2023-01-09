#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_USVoteFoundation import SourceUSVoteFoundation

if __name__ == "__main__":
    source = SourceUSVoteFoundation()
    launch(source, sys.argv[1:])

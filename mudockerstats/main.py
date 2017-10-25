import asyncio
import json
import logging
from os import environ as ENV

import jsonapi_requests
from aiohttp import web
from aiohttp.client_exceptions import ClientConnectionError
from aiosparql.client import SPARQLClient
from aiosparql.syntax import (IRI, RDF, Node, RDFTerm, Triples, escape_any,
                              escape_string)

logger = logging.getLogger(__name__)


if ENV.get("ENV", "prod").startswith("dev"):
    logging.basicConfig(level=logging.DEBUG)
else:
    logging.basicConfig(level=logging.INFO)


class Application(web.Application):
    sparql_timeout = 60
    run_command_timeout = 60

    @property
    def sparql(self):
        """
        The SPARQL client
        """
        if not hasattr(self, '_sparql'):
            self._sparql = SPARQLClient(ENV['MU_SPARQL_ENDPOINT'],
                                        graph=IRI(ENV['MU_APPLICATION_GRAPH']),
                                        loop=self.loop,
                                        read_timeout=self.sparql_timeout)
        return self._sparql






app = Application()

import asyncio
import json
import logging
from os import environ as ENV

import jsonapi_requests
from aiohttp import web
from aiohttp.client_exceptions import ClientConnectionError
from aiosparql.client import SPARQLClient
from aiosparql.syntax import IRI, escape_string

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


    async def get_service_stats(self, pipeline, service_name):
        """
        Get the CPU & Memory stats of the most recent stats event
        linked to a given service inside a pipeline.

        Arguments:
            pipeline: name of the pipeline containing the service
            service_name: service name

        Returns: An object with the form:
            {
            'systemCpuUsage':{
               'type':'typed-literal',
               'datatype':'http://www.w3.org/2001/XMLSchema#integer',
               'value':'93887870000000'
            },
            'totalUsage':{
               'type':'typed-literal',
               'datatype':'http://www.w3.org/2001/XMLSchema#integer',
               'value':'3003160973'
            },
            (...etc...)
         }

        """
        stats_service_name = "/{}_{}_1".format(pipeline, service_name)
        result = await self.sparql.query("""
            PREFIX swarmui: <http://swarmui.semte.ch/vocabularies/core/>
            SELECT DISTINCT ?systemCpuUsage ?totalUsage ?presystemCpuUsage ?pretotalUsage ?memoryUsage ?memoryLimit
            FROM {{graph}}
            WHERE {
                ?stats a swarmui:Stats .
                ?stats swarmui:name {{name}} .
                ?stats swarmui:read ?readdate .
                ?stats swarmui:cpuStats ?cpuStats .
                ?stats swarmui:precpuStats ?precpuStats .
                ?stats swarmui:memoryStats ?memoryStats .

                ?memoryStats swarmui:usage ?memoryUsage .
                ?memoryStats swarmui:limit ?memoryLimit .

                ?cpuStats swarmui:systemCpuUsage ?systemCpuUsage .
                ?cpuStats swarmui:cpuUsage ?cpuUsage .
                ?cpuUsage swarmui:totalUsage ?totalUsage .

                ?precpuStats swarmui:systemCpuUsage ?presystemCpuUsage .
                ?precpuStats swarmui:cpuUsage ?precpuUsage .
                ?precpuUsage swarmui:totalUsage ?pretotalUsage .
            }
            ORDER BY DESC(?readdate)
            LIMIT 1
            """, name=escape_string(stats_service_name))
        return result['results']['bindings'][0]


    async def handle_get_service_stats(self, request):
        """
        Handle a get request and return the stats of a given
        service in a JSON-API compliant format
        
        Arguments:
            request: the request object
        """
        try:
            pipeline = request.GET['pipeline']
            service_name = request.GET['service']
            service_stats = await self.get_service_stats(pipeline, service_name)
            logger.info(service_stats)
            return web.Response(text="zi")
        except KeyError:
            raise web.HTTPInternalServerError(body=json.dumps({
                "status": 500,
                "title": "Wrong query parameters",
                "detail": "Wrong query parameters"
            }))
        return

    async def handle_get_pipeline_stats(self, request):
        return


app = Application()
app.router.add_get('/stats/service', app.handle_get_service_stats)
app.router.add_get('/stats/pipeline', app.handle_get_pipeline_stats)

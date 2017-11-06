import asyncio
import json
import logging
import uuid
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
    """
    The main application class.
    """
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

        :pipeline: name of the pipeline containing the service
        :service_name: service name

        Notes:
            If the stack is created using a docker-compose file already
            in the DB, the pipeline will be empy since service names
            will have the form '/service' , while if they are created from
            a url, they will have the form '/pipeline_service_1'

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
        stats_service_name = "/{}_{}_1".format(pipeline.lower(), service_name) if pipeline is not None else "/{}".format(service_name)
        result = await self.sparql.query("""
            PREFIX swarmui: <http://swarmui.semte.ch/vocabularies/core/>
            SELECT DISTINCT ?systemCpuUsage ?totalUsage ?presystemCpuUsage ?pretotalUsage ?memoryUsage ?memoryLimit count(?perCpuUsage) as ?countPerCpuUsage
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
                ?cpuUsage swarmui:percpuUsage ?perCpuUsage .

                ?precpuStats swarmui:systemCpuUsage ?presystemCpuUsage .
                ?precpuStats swarmui:cpuUsage ?precpuUsage .
                ?precpuUsage swarmui:totalUsage ?pretotalUsage .
            }
            ORDER BY DESC(?readdate)
            LIMIT 1
            """, name=escape_string(stats_service_name))
        stats = result['results']['bindings'][0]
        return { stat: stats[stat]['value'] for stat in stats }


    async def get_json_stats(self, service_stats):
        """
        Return a JSON-API representation of the calculated
        CPU and memory stats for a given service:
            - % CPU
            - Memory used
            - Memory limit
            - % Memory

        :service_stats: service stats extracted from the DB.

        Return: json-api object
        """
        cpuPercent = 0.0
        try:
            cpuDelta = float(service_stats['totalUsage']) - float(service_stats['pretotalUsage'])
            systemDelta = float(service_stats['systemCpuUsage']) - float(service_stats['presystemCpuUsage'])
            memoryUsage = float(service_stats['memoryUsage'])
            memoryLimit = float(service_stats['memoryLimit'])
            perCpuUsage = int(service_stats['countPerCpuUsage'])
        except ValueError:
            return json.dumps({
                "status": 500,
                "title": "Error converting into float",
                "detail": "Error converting into float"
            })

        if (systemDelta > 0.0 and cpuDelta > 0.0):
            cpuPercent = (cpuDelta / systemDelta) * float(perCpuUsage) * 100.0

        return json.dumps({
            'data': {
                'type': 'service-stats',
                'id': uuid.uuid4().hex,
                'attributes': {
                    'cpu-percentage': cpuPercent,
                    'mem-usage': memoryUsage,
                    'mem-limit': memoryLimit,
                    'mem-percentage': memoryUsage / memoryLimit * 100.0
                }
            }
        })


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
            json_response = await self.get_json_stats(service_stats)
            return web.Response(body=json_response)
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

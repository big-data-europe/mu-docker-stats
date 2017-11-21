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
    sparql_timeout = 600
    run_command_timeout = 600

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


    async def get_service_stats(self, pipelines, service_names):
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
        stats_service_names = ['/{}_{}_1'.format(pair[0].lower(), pair[1]) for pair in zip(pipelines, service_names)]
        escaped_result = ", ".join(map(escape_string, stats_service_names))
        result = await self.sparql.query("""
            PREFIX swarmui: <http://swarmui.semte.ch/vocabularies/core/>
            SELECT DISTINCT ?name ?readdate ?systemCpuUsage ?totalUsage ?presystemCpuUsage ?pretotalUsage count(?perCpuUsage) as ?countPerCpuUsage ?memoryUsage ?memoryLimit
            WHERE
            {
                {
                    SELECT DISTINCT ?name MAX(?readdate) as ?readdate
                    WHERE
                    {
                        ?stats a swarmui:Stats .
                        ?stats swarmui:name ?name .
                        FILTER(?name IN({{escaped_result}}))
                        ?stats swarmui:read ?readdate .
                    }
                    GROUP BY ?name
                }
                ?stats a swarmui:Stats .
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
            """, escaped_result=escaped_result)
        stats = result['results']['bindings']
        result = [{ 'name': service['name']['value'], 
                    'stats': [ {serv: service[serv]['value'] for serv in service 
                                                             if serv != 'name'}] } 
                  for service in stats]
        return result


    def calculate_stats(self, stats):
        """
        Calculate cpu stats and return a  dictionary with the data.
        """
        cpuPercent = 0.0
        try:
            cpuDelta = float(stats['totalUsage']) - float(stats['pretotalUsage'])
            systemDelta = float(stats['systemCpuUsage']) - float(stats['presystemCpuUsage'])
            memoryUsage = float(stats['memoryUsage'])
            memoryLimit = float(stats['memoryLimit'])
            perCpuUsage = int(stats['countPerCpuUsage'])
        except ValueError:
            return json.dumps({
                "status": 500,
                "title": "Error converting into float",
                "detail": "Error converting into float"
            })

        if (systemDelta > 0.0 and cpuDelta > 0.0):
            cpuPercent = (cpuDelta / systemDelta) * float(perCpuUsage) * 100.0
        
        return {
            'cpu-percentage': cpuPercent,
            'mem-usage': memoryUsage,
            'mem-limit': memoryLimit,
            'mem-percentage': memoryUsage / memoryLimit * 100.0
        }


    async def get_stats_object(self, service_stats):
        """
        Return a JSON-API representation of the calculated
        CPU and memory stats for a given service:
            - % CPU
            - Memory used
            - Memory limit
            - % Memory
            - Time of the read

        :service_stats: service stats extracted from the DB.

        Return: json-api object
        """
        result = [ {'name': service['name'], 
                    'stats': [(lambda x, y: (x.update(y), x))(self.calculate_stats(stat), 
                                                             { 'read-date': stat['readdate']})[1] 
                               for stat in service['stats']] } 
                for service in service_stats]
        return result


    async def handle_get_service_stats(self, request):
        """
        Handle a get request and return the stats of a given
        service in a JSON-API compliant format

        Arguments:
            request: the request object
        """
        try:
            pipelines = request.GET['pipelines'].split(',')
            service_names = request.GET['services'].split(',')
            service_stats = await self.get_service_stats(pipelines, service_names)
            stats_object = await self.get_stats_object(service_stats)
            return web.json_response(stats_object, content_type='application/json')
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
app.router.add_get('/stats', app.handle_get_service_stats)

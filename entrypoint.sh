#!/bin/bash

[ -n "$TOX" ] && pip install tox

[ -n "$1" ] && exec "$@"

if [ "${ENV:0:3}" == dev ]; then
	pip install aiohttp-devtools
	exec adev runserver -p $PORT --no-pre-check /src/mudockerstats
fi

exec /src/run.py

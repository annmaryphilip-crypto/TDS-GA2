#!/bin/bash

# Start Redis server in the background
redis-server --daemonize yes

# Start FastAPI
uvicorn main:app --host 0.0.0.0 --port ${PORT:-10000}
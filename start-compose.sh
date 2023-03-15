#!/usr/bin/env bash
docker-compose -f docker-compose.yml -f dc.$1.yml ${@:2}
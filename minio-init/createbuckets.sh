#!/bin/sh
mc alias set local http://minio:9000 minioadmin minioadmin
mc mb local/my-bucket
mc policy set public local/my-bucket


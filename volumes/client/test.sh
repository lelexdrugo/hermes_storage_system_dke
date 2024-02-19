#!/bin/bash

res = $(nc hermes 65000 <<EOF
GET /actuator/health HTTP/1.0
Host: http://hermes:65000
EOF
)
res = $(echo $res | grep "UP")
#!/bin/bash

exec 3<>/dev/tcp/hermes/8080
printf "GET /actuator/health HTTP/1.0\r\n\r\n" >&3
#cat <&3 &
# Search for status and up. If something is returned it is success, so is like a 0 and command after || (or) is not executed
# If nothing is returned, it is like a 1 and command after || (or) is executed
timeout 1 cat <&3 | grep status | grep UP || exit 1
exit 0
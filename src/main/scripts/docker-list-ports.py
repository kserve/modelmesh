#!/usr/bin/env python3

# Copyright 2021 IBM Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This script targets Ubuntu 14.04 or later system, and therefore uses Python 3.4.

import sys, os, json, urllib.request, urllib.parse, http.client, socket

DOCKER_SOCKET_FILE = '/docker.sock'

# A subclass of HTTPConnection that lets us perform HTTP operations over a
# Unix-domain socket.  We'll use this to communicate with the Docker daemon.
class UnixHTTPConnection(http.client.HTTPConnection):
    def __init__(self, path, host='localhost', port=None, timeout=None):
        http.client.HTTPConnection.__init__(self, host, port=port, timeout=timeout)
        self.path = path

    def connect(self):
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(self.path)
        self.sock = sock

def get_docker_connection():
    return UnixHTTPConnection(DOCKER_SOCKET_FILE)

hostname = os.getenv('HOSTNAME')
if hostname is None:
    print('The HOSTNAME environment variable must be set.', file=sys.stderr)
    sys.exit(1)

connection = get_docker_connection()
connection.request('GET', '/containers/{}/json'.format(urllib.parse.quote(hostname)))
response = connection.getresponse()
if response.status != 200:
    print('Error fetching information on this Docker container.', file=sys.stderr)
    print('HTTP status {:d}: {}'.format(response.status, response.reason), file=sys.stderr)
    sys.exit(1)

info = json.loads(response.read().decode('utf-8'))
connection.close()

network_settings = info.get('NetworkSettings')
if not network_settings:
    print('No information on network settings found in the container information.', file=sys.stderr)
    sys.exit(1)

port_info = network_settings.get('Ports')
if not port_info:
    print('The container has no exposed ports.', file=sys.stderr)
    sys.exit(1)

for port, host_port_info in port_info.items():
    if host_port_info is None:
        print('Port {} isn\'t published to the host.'.format(port), file=sys.stderr)
        continue
    for host_port_item in host_port_info:
        host_port = host_port_item.get('HostPort')
        if host_port is not None:
            print('{} {}'.format(port, host_port))

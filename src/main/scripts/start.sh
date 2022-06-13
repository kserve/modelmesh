#!/bin/bash

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

# Capture logs at the earliest opportunity so that output from this script is
# captured also.
if [[ -d "logvol" && -n "$WKUBE_POD_NAME" ]]; then
    mkdir -p "logvol/$WKUBE_POD_NAME"
fi

ENV_CONFIG=/etc/environment
while read -r line
do
   echo "Exporting environment variable :"$line
   export $line
done < $ENV_CONFIG

if [ "$MM_INSTALL_PATH" = "" ]; then
	MM_INSTALL_PATH="$PWD"
fi

echo "Running as $(id): $(whoami)"
echo "PATH=$PATH"
echo "LD_LIBRARY_PATH=$LD_LIBRARY_PATH"
echo "PKG_CONFIG_PATH=$PKG_CONFIG_PATH"
echo "XDG_DATA_DIRS=$XDG_DATA_DIRS"
echo

: ${SERVICE_ARGS:=""}
PRIVATE_ENDPOINT=''
LOG_CONFIG_ARG="-Dlog4j.configurationFile=$MM_INSTALL_PATH/lib/log4j2.xml"
# Enable garbage-free log4j ThreadContext
LOG_PERF_ARGS="-Dlog4j2.enable.threadlocals=true -Dlog4j2.garbagefree.threadContextMap=true"

if [[ -e "/var/run/secrets/kubernetes.io/serviceaccount" ]]; then
	# When running under Kubernetes, the pod specification is expected to
	# use the "downward api" to set the following env vars:
	#
	#  WKUBE_POD_NAME: The pod's name, valueFrom: fieldRef: fieldPath: metadata.name
	#  WKUBE_POD_IPADDR: The pod's IP address, valueFrom: fieldRef: fieldPath: status.podIP
	#
	# If just these variables are specified, the pod's IP address will be
	# registered in Litelinks as the Litelinks service instance's address,
	# and 8080 will be registered as the port.  In other words, the model-mesh
	# service will be available only from inside the cluster.  To make model-mesh
	# available from outside the cluster, create a Kubernetes service of
	# type NodePort and set the following variables as well.  In this
	# case, the IP address of the Kubernetes node on which the pod is
	# running will be used as the Litelinks service instance's address and
	# the NodePort port as the port.  The pod's IP address and 8080 will
	# be registered as the "private" endpoint, reachable only by other
	# Litelinks clients in the same Kubernetes cluster.
	#
	#  WKUBE_NODE_IP: The node ip address, valueFrom: fieldRef: fieldPath: status.hostIP
	#  WKUBE_NODE_NAME: The node name, valueFrom: fieldRef: fieldPath: spec.nodeName
	#  WKUBE_SERVICE_NAME: The service name of the NodePort service whose
	#                      node port should be used for access from outside
	#                      the cluster.
	#  WKUBE_SERVICE_PORT_NAME: The name of the node port from
	#                      $WKUBE_SERVICE_NAME to look up.

	echo "Running in Kubernetes"
	if [[ -z "$WKUBE_POD_NAME" || -z "$WKUBE_POD_IPADDR" ]]; then
		echo "These environment variables must be set (using the Kubernetes downward API):" >&2
		echo " WKUBE_POD_NAME, WKUBE_POD_IPADDR" >&2
		exit 1
	fi

	#If node ip is set but not mmlocation then set it
	if [[ -z "$MM_LOCATION" && -n "$WKUBE_NODE_IP" ]]; then
		export MM_LOCATION="$WKUBE_NODE_IP"
	fi

	if [[ ( -n "$WKUBE_NODE_IP" || -n "$WKUBE_NODE_NAME" ) && -n "$WKUBE_SERVICE_NAME" ]]; then
		echo "Registering both external (NodePort) and internal (pod) endpoints"
		# Use node ip if provided, otherwise fall back to lookup using node name
		if [[ -n "$WKUBE_NODE_IP" ]]; then
			WATSON_SERVICE_ADDRESS="$WKUBE_NODE_IP"
		else
			WATSON_SERVICE_ADDRESS=$(kube-info resolve-node "$WKUBE_NODE_NAME")
		fi

		PORT_8080=$(kube-info nodeport-for-service -p "$WKUBE_SERVICE_PORT_NAME" "$WKUBE_SERVICE_NAME")
		PRIVATE_ENDPOINT="$WKUBE_POD_IPADDR:8080"
	else
		echo "Registering internal (pod) endpoint only"
		WATSON_SERVICE_ADDRESS="$WKUBE_POD_IPADDR"
		PORT_8080="8080"
	fi

    SERVICE_ARGS="-p 8080 -r $PORT_8080"
elif [[ -e "/.dockerenv" && -S "./docker.sock" ]]; then
    # If running otherwise in Docker, attempt to lookup ports to use for listening and registration
    echo "Running in Docker (found /.dockerenv)"
    if [ -n "$PORT_8080" ]; then
        #If running in Mesos/Marathon then this environment variable should be set
        echo "Running in Mesos/Marathon"
        WATSON_SERVICE_ADDRESS="$HOST"
    elif [ -f "${MM_INSTALL_PATH}/docker-list-ports.py" ]; then
        PORT_INFO=$(${MM_INSTALL_PATH}/docker-list-ports.py)
        RC=$?
        if [ "$RC" = "0" ]; then
            ## format: "containerport/tcp hostport"
            PORT_8080=$(echo "$PORT_INFO" | awk '/^8080\/tcp/ {print $2}')
        else
            echo "docker-list-ports.py command failed with exit code $RC" >&2
            # This should be considered fatal
            exit 1
        fi
    else
        echo "warning: could not determine host port, using 8080" >&2
        PORT_8080="8080"
    fi
    SERVICE_ARGS="-p 8080 -r $PORT_8080"
fi


echo "PORT_ARGS=${SERVICE_ARGS}"
echo "WATSON_SERVICE_ADDRESS=${WATSON_SERVICE_ADDRESS}"

if [[ -n "$PRIVATE_ENDPOINT" ]]; then
    PRIVATE_ENDPOINT_ARG="-Dlitelinks.private_endpoint=$PRIVATE_ENDPOINT"
fi



# If running in platform ASC, set instance id to EGO instanceid
if [ "$EGOSC_INSTANCE_SEQNO" != "" ]; then
	SERVICE_SUFFIX="-service"
	APP_NAME="${EGOSC_SERVICE_NAME::-${#SERVICE_SUFFIX}}"
	APP_NAME_END="${APP_NAME:${#APP_NAME}<6?0:-6}"
	INST_ID="${APP_NAME_END}_${EGOSC_INSTANCE_SEQNO}"
	SERVICE_ARGS="$SERVICE_ARGS -i ${INST_ID}"
	echo "Registering instance id (from EGO) as \"${INST_ID}\""
elif [[ -n "$WKUBE_POD_NAME" ]]; then
    # Use the pod's name, which is unique in the Kubernetes cluster,
    # for the Litelinks instance id. In fact just the last 12 chars
    # are used, since these should be unique per logical model-mesh cluster
    # (kube deployment). It's preferable for this to be short because
    # of the amount of references to it which are written to the
    # model registry in zookeeper or etcd.
    POD_NAME_END="${WKUBE_POD_NAME:${#WKUBE_POD_NAME}<12?0:-12}"
    INST_ID="$POD_NAME_END"
    SERVICE_ARGS="$SERVICE_ARGS -i $INST_ID"
    echo "Registering instance id (from Kubernetes pod name) as \"$INST_ID\""
elif [ "$MESOS_TASK_ID" != "" ]; then
	echo MARATHON_APP_ID=${MARATHON_APP_ID}
	echo MESOS_TASK_ID=${MESOS_TASK_ID}
	INST_ID="${MESOS_TASK_ID}"
	SERVICE_ARGS="$SERVICE_ARGS -i ${INST_ID}"
	echo "Registering instance id (from M/M) as \"${INST_ID}\""
fi

if [[ "$MM_ENABLE_SSL" != "false" && "$MM_SVC_GRPC_PRIVATE_KEY_PATH" != "" ]]; then
    echo "MM_INTRACLUSTER_CLIENTAUTH=${MM_INTRACLUSTER_CLIENTAUTH}"
    if [[ "$MM_INTRACLUSTER_CLIENTAUTH" != "false" ]]; then
        SERVICE_ARGS="$SERVICE_ARGS -e ssl-ca"
        echo "Litelinks intracluster TLS: Client Authentication is enabled"
    else
        SERVICE_ARGS="$SERVICE_ARGS -e ssl"
        echo "Litelinks intracluster TLS: Client Authentication is DISABLED"
    fi
else
    echo "WARNING: MM_SVC_GRPC_PRIVATE_KEY_PATH not set *AND/OR* MM_ENABLE_SSL=false, using PLAINTEXT for internal comms"
fi

echo "SERVICE_VERSION=${SERVICE_VERSION}"

# Look for version in build-version file first, then SERVICE_VERSION env var
# If found in either, include in litelinks registration
VERS_PROP=$(<${MM_INSTALL_PATH}/build-version)
if [ "$?" = "0" ]; then
	SVC_VERS="$(echo ${VERS_PROP} | tr -d '\r' | cut -d '=' -f 2- | cut -d ' ' -f 1)"
	echo "build-version=${SVC_VERS}"
else
	SVC_VERS="${SERVICE_VERSION}"
fi
if [ "$SVC_VERS" != "" ]; then
	SERVICE_ARGS="${SERVICE_ARGS} -v ${SVC_VERS}"
	echo "Registering ModelMesh Service version as \"${SVC_VERS}\""
fi


#If a parameter was passed then use it as the model-mesh service name
: ${MM_SERVICE_NAME:="$1"}

if [ "$MM_SERVICE_NAME" = "" ]; then
	MM_SERVICE_NAME="$TAS_SERVICE_NAME"
	if [ "$MM_SERVICE_NAME" = "" ]; then
		echo "No model-mesh service name provided" >&2
		exit 1
	fi
	echo "WARNING: TAS_SERVICE_NAME env var is deprecated, use MM_SERVICE_NAME instead"
fi

export MM_SERVICE_NAME
ANCHOR_FILE="$MM_INSTALL_PATH/model-mesh.anchor"

cd "$MM_INSTALL_PATH"

if [ -z "$JAVA_HOME" ]; then
	if [ -z "$DEFAULT_JAVA_HOME" ]; then
		echo "JAVA_HOME not set" >&2
		exit 1
	else
		echo "JAVA_HOME not set, setting to default: ${DEFAULT_JAVA_HOME}"
		export JAVA_HOME=${DEFAULT_JAVA_HOME}
	fi
else
	echo "JAVA_HOME set to $JAVA_HOME"
fi
echo "Java version information:"
$JAVA_HOME/bin/java -version

# use IP address for registration, not hostname
if [ "$WATSON_SERVICE_ADDRESS" = "" ]; then
	WATSON_SERVICE_ADDRESS="$(hostname -I | cut -f1 -d' ')"
fi
export WATSON_SERVICE_ADDRESS

echo KV_STORE=$KV_STORE
echo LL_REGISTRY=$LL_REGISTRY
echo ZOOKEEPER=$ZOOKEEPER
echo WATSON_SERVICE_ADDRESS=$WATSON_SERVICE_ADDRESS
echo MM_SERVICE_NAME=$MM_SERVICE_NAME
echo PRIVATE_ENDPOINT=$PRIVATE_ENDPOINT
echo MM_LOCATION=$MM_LOCATION

if [ "$MM_SERVICE_CLASS" = "" ]; then
   MM_SERVICE_CLASS="com.ibm.watson.modelmesh.SidecarModelMesh"
fi
echo MM_SERVICE_CLASS=$MM_SERVICE_CLASS

if [ "$MM_SERVICE_CLASS" != "com.ibm.watson.modelmesh.SidecarModelMesh" ]; then
   STRING_SCALE_JVM_ARGS="-XX:+UseStringDeduplication -XX:StringTableSize=2000003"
else
   echo "INTERNAL_GRPC_PORT=${INTERNAL_GRPC_PORT:-8085}" # should match default in SidecarModelMesh
fi

echo SERVICE_ARGS=$SERVICE_ARGS

TRUSTSTORE_PASS="watson15qa"
TRUSTSTORE_ARG="-Dwatson.ssl.truststore.path=${MM_INSTALL_PATH}/lib/truststore.jks -Dwatson.ssl.truststore.password=${TRUSTSTORE_PASS}"

# If provided, import mmesh grpc key cert into litelinks truststore
TLS_KEY_CERT_PATH=${MM_TLS_KEY_CERT_PATH-$MM_SVC_GRPC_CA_CERT_PATH}
if [[ "$TLS_KEY_CERT_PATH" != "" && -f "$TLS_KEY_CERT_PATH" ]]; then
   $JAVA_HOME/bin/keytool -importcert -file "$TLS_KEY_CERT_PATH" \
       -keystore ${MM_INSTALL_PATH}/lib/truststore.jks -alias mmesh_grpc -noprompt -storepass ${TRUSTSTORE_PASS}
   echo "Imported provided CA cert into litelinks truststore: $TLS_KEY_CERT_PATH"

   TLS_PRIV_KEY_PATH=${MM_TLS_PRIVATE_KEY_PATH-$MM_SVC_GRPC_PRIVATE_KEY_PATH}
   if [[ "$MM_ENABLE_SSL" != "false" && "$TLS_PRIV_KEY_PATH" != "" ]]; then
       SSL_PK_ARG="-Dlitelinks.ssl.key.path=${TLS_PRIV_KEY_PATH} -Dlitelinks.ssl.key.certpath=${TLS_KEY_CERT_PATH}"
       echo "Using provided private key for litelinks (internal) TLS: $TLS_PRIV_KEY_PATH"
       TLS_PRIV_KEY_PASS=${MM_TLS_PRIVATE_KEY_PASSPHRASE-$MM_SVC_GRPC_PRIVATE_KEY_PASSPHRASE}
       if [[ "$TLS_PRIV_KEY_PASS" != "" ]]; then
           # Pass this via env var so that it doesn't show on the command line
           export LITELINKS_SSL_KEY_PASSWORD="${TLS_PRIV_KEY_PASS}"
           echo "Using provided passphrase for private key"
       fi
   fi
fi

# Also import any additional trust certs (used in particular for when migrating to new key/cert pairs).
# The env var can point to one or more (comma-separated) file (single cert) or dir containing .pem suffixed files
INTERNAL_TRUST_CERTS=${MM_INTERNAL_TRUST_CERTS-$MM_TRUST_CERTS}
if [[ "$INTERNAL_TRUST_CERTS" != "" ]]; then
    export LITELINKS_SSL_TRUSTCERTS_PATH="$INTERNAL_TRUST_CERTS"
    echo "Will use provided trust certs for internal litelinks comms at path(s): $INTERNAL_TRUST_CERTS"

    # Logic below unnecessary since we just pass the env var value to litelinks, but leaving commented for now
    #if [[ -d "$INTERNAL_TRUST_CERTS" ]]; then
	#	for cert_path in $(ls -d "$INTERNAL_TRUST_CERTS/*.pem"); do
	#	    $JAVA_HOME/bin/keytool -importcert -file "$cert_path" \
    #            -keystore ${MM_INSTALL_PATH}/lib/truststore.jks -alias mmesh_grpc -noprompt -storepass ${TRUSTSTORE_PASS}
	#	    echo "Imported additional trust cert into litelinks truststore: $cert_path"
	#	done
    #elif [[ -f "$INTERNAL_TRUST_CERTS" ]]; then
    #    $JAVA_HOME/bin/keytool -importcert -file "$INTERNAL_TRUST_CERTS" \
    #       -keystore ${MM_INSTALL_PATH}/lib/truststore.jks -alias mmesh_grpc -noprompt -storepass ${TRUSTSTORE_PASS}
    #   echo "Imported additional trust cert into litelinks truststore: $INTERNAL_TRUST_CERTS"
    #fi
fi

KUBE_CRT="/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"

# import Kubernetes CA certificate (in case this is used to sign mmesh certs in future)
if [[ -f "$KUBE_CRT" ]]; then
   $JAVA_HOME/bin/keytool -importcert -file "$KUBE_CRT" \
       -keystore ${MM_INSTALL_PATH}/lib/truststore.jks -alias kube_ca -noprompt -storepass ${TRUSTSTORE_PASS}
   echo "Imported Kubernetes CA certificate into litelinks truststore: ${KUBE_CRT}"
else
   echo "Kubernetes CA certificate not found: ${KUBE_CRT}"
fi

echo $$ > ${ANCHOR_FILE}

if [ "$MEM_LIMIT_MB" = "" ]; then
	DOCKER_LIM_FILE="/sys/fs/cgroup/memory/memory.limit_in_bytes"

	if [ -e "${DOCKER_LIM_FILE}" ]; then
		MEM_LIMIT_MB=$(($(cat ${DOCKER_LIM_FILE})/1024/1024))
		echo "Using process mem limit of ${MEM_LIMIT_MB}MiB from ${DOCKER_LIM_FILE}"
	else
    	MEM_LIMIT_MB="1536"
    	echo "No process mem limit provided or found, defaulting to ${MEM_LIMIT_MB}MiB"
	fi
fi

echo "MEM_LIMIT_MB=${MEM_LIMIT_MB}"

if [ "$HEAP_SIZE" != "" ]; then
	echo "** NOTE: HEAP_SIZE env var is deprecated, please remove it"
fi

if [ "$HEAP_SIZE_MB" = "" ]; then
    # Default heap size to MIN(41% of mem-limit, 640MiB)
    HS=$((${MEM_LIMIT_MB}*41/100))
    HEAP_SIZE_MB=$(($HS<640 ? $HS : 640))
    echo "Using default heap size of MIN(41% of MEM_LIMIT_MB, 640MiB) = ${HEAP_SIZE_MB}MiB"
fi
echo "HEAP_SIZE_MB=${HEAP_SIZE_MB}"

# Reserved headroom for JVM internals: 64MiB + 20% of heapsize
MEM_HEADROOM_MB=$((64+${HEAP_SIZE_MB}/5))
echo "MEM_HEADROOM_MB=${MEM_HEADROOM_MB}"

MAX_GC_PAUSE=${MAX_GC_PAUSE:-50}
echo "MAX_GC_PAUSE=${MAX_GC_PAUSE} millisecs"

MAX_DIRECT_BUFS_MB=$((${MEM_LIMIT_MB}-${HEAP_SIZE_MB}-192))
echo "MAX_DIRECT_BUFS_MB=${MAX_DIRECT_BUFS_MB}"

# litelinks kubernetes health probe - defaults to port 8089
if [ "$MM_PROBE_PORT" != "NONE" ]; then
	LL_PROBE_ARG="-h ${MM_PROBE_PORT:=8089}"
	echo "Health probe HTTP endpoint will use port ${MM_PROBE_PORT}"
else
	echo "Health probe HTTP endpoint disabled"
fi

# disable per-invocation logging and sending dest-id internally by default
# (can be re-enabled via kv-store config parameter)
export MM_LOG_EACH_INVOKE="${MM_LOG_EACH_INVOKE:-false}"
export MM_SEND_DEST_ID="${MM_SEND_DEST_ID:-false}"

# this is required to limit the amount of native mem used by the G1 garbage collector
export MALLOC_ARENA_MAX=4

SHUTDOWN_TIMEOUT_MS=${SHUTDOWN_TIMEOUT_MS:-30000}
echo "SHUTDOWN_TIMEOUT_MS=$SHUTDOWN_TIMEOUT_MS"

LITELINKS_ARGS="-Dlitelinks.cancel_on_client_close=true -Dlitelinks.threadcontexts=log_mdc -Dlitelinks.shutdown_timeout_ms=${SHUTDOWN_TIMEOUT_MS} -Dlitelinks.produce_pooled_bytebufs=true"

# have litelinks use OpenSSL instead of JDK TLS implementation (faster)
LL_OPENSSL_ARG="-Dlitelinks.ssl.use_jdk=false"

# These two args are needed to use netty's off-the-books direct buffer allocation
NETTY_DIRECTBUF_ARGS="-Dio.netty.tryReflectionSetAccessible=true --add-opens=java.base/java.nio=ALL-UNNAMED"
# this defaults to equal max heap, which can result in container OOMKilled
MAX_DIRECT_BYTES="$((${MAX_DIRECT_BUFS_MB}*1024*1024))"
# Java native direct memory setting shouldn't be used since all
# direct buffers are allocated by netty, so set it small (32MiB)
JAVA_MAXDIRECT_ARG="-XX:MaxDirectMemorySize=33554432"
# This defaults to match the java setting so we set it explicitly to desired val
NETTY_MAXDIRECT_ARG="-Dio.netty.maxDirectMemory=${MAX_DIRECT_BYTES}"
# Ensure that grpc-java shares a single pooled buffer allocator with other netty usage (e.g. litelinks)
GRPC_USE_SHARED_ALLOC_ARG="-Dio.grpc.netty.useCustomAllocator=false"

# Can disable netty buffer bounds and refcount checks, for performance (enabled by default)
if [ "false" = "${NETTY_BUFFER_CHECKS}" ]; then
   NETTY_DISABLE_CHECK_ARGS="-Dio.netty.buffer.checkAccessible=false -Dio.netty.buffer.checkBounds=false"
fi

if [ "true" = "${GC_DIAG}" ]; then
	GC_DIAG_ARGS="-Xlog:gc+ref*=debug -Xlog:gc+ergo*=debug -Xlog:gc+age*=debug -XX:+PrintFlagsFinal"
fi

# ensure litelinks is first on classpath (should be before libthrift)
LL_JAR="$(ls lib/litelinks-core-*.jar)"

# disable java FIPS which breaks when TLS is enabled
if [[ "$CUSTOM_JVM_ARGS" != *-Dcom.redhat.fips=* ]]; then
  DISABLE_FIPS_ARG="-Dcom.redhat.fips=false"
fi

# print command that's about to be run
set -x

exec $JAVA_HOME/bin/java -cp "$LL_JAR:lib/*" -XX:+UnlockExperimentalVMOptions -XX:+UseG1GC \
 -XX:MaxGCPauseMillis=${MAX_GC_PAUSE} \
 -XX:-ResizePLAB -Xmx${HEAP_SIZE_MB}m -Xms${HEAP_SIZE_MB}m \
 ${STRING_SCALE_JVM_ARGS} \
 -XX:MaxInlineLevel=28 \
 -Xlog:gc:"${MM_INSTALL_PATH}/log/vgc_${HOSTNAME}.log" ${GC_DIAG_ARGS} \
 -Dfile.encoding=UTF8 \
 ${NETTY_DIRECTBUF_ARGS} \
 ${DISABLE_FIPS_ARG} \
 ${JAVA_MAXDIRECT_ARG} ${NETTY_MAXDIRECT_ARG} ${NETTY_DISABLE_CHECK_ARGS} \
 ${GRPC_USE_SHARED_ALLOC_ARG} \
 ${SSL_PK_ARG} ${TRUSTSTORE_ARG} ${LITELINKS_ARGS} ${CUSTOM_JVM_ARGS} \
 $LL_OPENSSL_ARG \
 $PRIVATE_ENDPOINT_ARG \
 $LOG_CONFIG_ARG $LOG_PERF_ARGS \
 com.ibm.watson.litelinks.server.LitelinksService \
 -s ${MM_SERVICE_CLASS} -n ${MM_SERVICE_NAME} -a ${ANCHOR_FILE} ${SERVICE_ARGS} ${LL_PROBE_ARG}

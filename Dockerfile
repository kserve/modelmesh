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

FROM registry.access.redhat.com/ubi8/ubi-minimal:8.4 as build_base

ARG ETCD_VERSION=v3.5.4

LABEL image="build_base"

USER root

RUN true \
    && microdnf --nodocs install java-17-openjdk-devel nss \
    && microdnf update --nodocs \
    && microdnf clean all \
    && sed -i 's:security.provider.12=SunPKCS11:#security.provider.12=SunPKCS11:g' /usr/lib/jvm/java-17-openjdk-*/conf/security/java.security \
    && sed -i 's:#security.provider.1=SunPKCS11 ${java.home}/lib/security/nss.cfg:security.provider.12=SunPKCS11 ${java.home}/lib/security/nss.cfg:g' /usr/lib/jvm/java-17-openjdk-*/conf/security/java.security \
    && true

RUN microdnf install wget tar \
    gzip vim-common python39 maven && \
    pip3 install -U pip setuptools

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk

# Install etcd -- used for CI tests
RUN wget -q https://github.com/etcd-io/etcd/releases/download/${ETCD_VERSION}/etcd-${ETCD_VERSION}-linux-amd64.tar.gz && \
    mkdir -p /usr/lib/etcd && \
    tar xzf etcd-*-linux-amd64.tar.gz -C /usr/lib/etcd --strip-components=1 --no-same-owner && \
    rm -rf etcd*.gz

ENV PATH="/usr/lib/etcd:$PATH"

# Copy in code
RUN mkdir /build

WORKDIR /build

###############################################################################
FROM build_base AS build

LABEL image="build"

COPY / /build

ENV MAVEN_OPTS="-Dfile.encoding=UTF8"

RUN mvn -B package -DskipTests=true --file pom.xml

###############################################################################
FROM registry.access.redhat.com/ubi8/ubi-minimal:8.4

ARG imageVersion
ARG buildId
ARG commitSha
ARG USER=2000

LABEL name="model-mesh" \
      vendor="KServe" \
      version="${imageVersion}" \
      summary="Core model-mesh sidecar image" \
      description="Model-mesh is a distributed LRU cache for serving runtime models" \
      release="${commitSha}"
LABEL maintainer=nickhill@us.ibm.com

USER root

RUN true \
    && microdnf --nodocs install java-17-openjdk-headless nss \
    && microdnf update --nodocs \
    && microdnf clean all \
    && sed -i 's:security.provider.12=SunPKCS11:#security.provider.12=SunPKCS11:g' /usr/lib/jvm/java-17-openjdk-*/conf/security/java.security \
    && sed -i 's:#security.provider.1=SunPKCS11 ${java.home}/lib/security/nss.cfg:security.provider.12=SunPKCS11 ${java.home}/lib/security/nss.cfg:g' /usr/lib/jvm/java-17-openjdk-*/conf/security/java.security \
    && true

ENV JAVA_HOME=/usr/lib/jvm/jre-17-openjdk

COPY --from=build /build/target/dockerhome/ /opt/kserve/mmesh/

# Make this the current directory when starting the container
WORKDIR /opt/kserve/mmesh

RUN microdnf install shadow-utils hostname python39 && \
    # Create app user
    useradd -c "Application User" -U -u ${USER} -m app && \
    chown -R app:0 /home/app && \
    # Adjust permissions on /etc/passwd to be writable by group root.
    # The user app is replaced by the assigned UID on OpenShift.
    chmod g+w /etc/passwd && \
    # In newer Docker there is a --chown option for the COPY command
    ln -s /opt/kserve/mmesh /opt/kserve/tas && \
    mkdir -p log && \
    chown -R app:0 . && \
    chmod -R 771 . && chmod 775 *.sh *.py && \
    echo "${buildId}" > /opt/kserve/mmesh/build-version && \
    \
    # Disable java FIPS - see https://access.redhat.com/documentation/en-us/openjdk/17/html-single/configuring_openjdk_17_on_rhel_with_fips/index#config-fips-in-openjdk
    sed -i 's/security.useSystemPropertiesFile=true/security.useSystemPropertiesFile=false/g' $JAVA_HOME/conf/security/java.security

EXPOSE 8080

# Run as non-root user by default, to allow runAsNonRoot:true without runAsUser
USER ${USER}

# The command to run by default when the container is first launched
CMD ["sh", "-c", "exec /opt/kserve/mmesh/start.sh"]

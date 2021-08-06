#!/bin/bash

# Fail on any error
set -e

ETCD_VERSION=v3.5.0
INSTALL_DIR="/usr/local/bin"

wget -q https://github.com/etcd-io/etcd/releases/download/${ETCD_VERSION}/etcd-${ETCD_VERSION}-linux-amd64.tar.gz
mkdir -p ${INSTALL_DIR}
tar xzf etcd-*-linux-amd64.tar.gz -C ${INSTALL_DIR} --strip-components=1
rm -rf etcd*.gz

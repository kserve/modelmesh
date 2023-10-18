## Enable TLS/SSL

TLS between the ModelMesh container and the model runtime container isn't currently required or supported, since the communication happens with a single pod over localhost.

However, TLS must be enabled in production deployments for the external gRPC service interfaces exposed by ModelMesh itself (which include your proxied custom gRPC interface).

To do this, you must provide both private key and corresponding cert files in pem format, volume-mounting them into the ModelMesh container from a kubernetes secret. TLS is then enabled by setting the values of the following env vars on the ModelMesh container to the paths of those mounted files as demonstrated [here](https://github.com/kserve/modelmesh/blob/main/config/base/patches/tls.yaml#L39-L42).

The same certificate pair will then also be used for "internal" communication between the model-mesh pods, which is unencrypted otherwise (in prior versions the internal traffic was encrypted unconditionally, but using "hardcoded" certs baked into the image which have now been removed).

## Client Authentication 

To additionally enable TLS Client Auth (aka Mutual Auth, mTLS):

- Set the `MM_TLS_CLIENT_AUTH` env var to either `REQUIRE` or `OPTIONAL` (case-insensitive)
- Mount pem-format cert(s) to use for trust verification into the container, and set the `MM_TLS_TRUST_CERT_PATH` to a comma-separated list of the mounted paths to these files

## Certificate Format

A `PKCS8` format key is required due to netty [only supporting PKCS8 keys](https://github.com/netty/netty/wiki/SslContextBuilder-and-Private-Key).

For a key cert pair, `server.crt` and `server.key`, you can convert an unencrypted `PKCS1` key to `PKCS8`.

```
$ openssl pkcs8 -topk8 -nocrypt -in server.key -out mmesh.key
```

If only one hash is displayed, they match. You can also use the above command to verify the original key cert pair `server.crt` and `server.key`.

### cert-manager
If you are using [cert-manager](https://github.com/cert-manager/cert-manager) on Kubernetes/OpenShift to generate certificates, just ensure that the `.spec.privateKey.encoding` field of your Certificate CR is set to `PKCS8` (it defaults to `PKCS1`).

## Updating and Rotating Private Keys

Because the provided certificates are also used for intra-cluster communication, care must be taken when updating to a new private key to avoid potential temporary impact to the service. All pods inter-communicate during rolling upgrade transitions, so the new pods must be able to connect to the old pods and vice versa. If new trust certs are required for the new private key, an update must be performed first to ensure both old and new trust certs are used, and these must both remain present for the subsequent key update. Note that these additional steps are not required if a common and unchanged CA certificate is used for trust purposes.

There is a dedicated env var `MM_INTERNAL_TRUST_CERTS` which can be used to specify additional trust (public) certificates for inter-cluster communication only. It can be set to one or more comma-separated paths which point to either individual pem-formatted cert files or directories containing certs with `.pem` and/or `.crt` extensions. These paths would correspond to Kube-mounted secrets. Here is an example of the three distinct updates required:

1. Add `MM_INTERNAL_TRUST_CERTS` pointing to the new cert:
```
- name: MM_TLS_KEY_CERT_PATH
  value: /path/to/existing-keycert.pem
- name: MM_TLS_PRIVATE_KEY_PATH
  value: /path/to/existing-key.pem
- name: MM_INTERNAL_TRUST_CERTS
  value: /path/to/new-cacert.pem
```
2. Switch to the new private key pair, with `MM_INTERNAL_TRUST_CERTS` now pointing to the old cert:
```
- name: MM_TLS_KEY_CERT_PATH
  value: /path/to/new-keycert.pem
- name: MM_TLS_PRIVATE_KEY_PATH
  value: /path/to/new-key.pem
- name: MM_INTERNAL_TRUST_CERTS
  value: /path/to/existing-keycert.pem
```
3. Optionally remove `MM_TRUST_CERTS`:
```
- name: MM_TLS_KEY_CERT_PATH
  value: /path/to/new-keycert.pem
- name: MM_TLS_PRIVATE_KEY_PATH
  value: /path/to/new-key.pem
```

**Note**: these additional steps shouldn't be required if either:

- The same CA is used for both the old and new public certs (so they are not self-signed)
- Some temporary service disruption is acceptable - this will likely manifest as some longer response times during the upgrade, possibly with some timeouts and failures. It should not persist beyond the rolling update process and the exact magnitude of the impact depends on various factors such as cluster size, loading time, request volume and patterns, etc.
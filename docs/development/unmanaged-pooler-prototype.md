# Catalog-configured unmanaged poolers

The operator provisions pooler-only source Pods through `Shard.spec.unmanagedPoolers`.
Each migration uses one managed authority cohort. Source Pods may span existing
cells and share the same immutable external source connection. Gateways must be
deployed with catalog routing support before source Pods are added.

```yaml
spec:
  migrationKeySecretRef:
    name: migration-catalog-key
    key: key
  unmanagedPoolers:
    source:
      connectionName: external
      cells: [cell1, cell2]
      replicasPerCell: 1
      connectionBudget: 32
      multipooler:
        resources:
          requests:
            cpu: 100m
            memory: 128Mi
```

Provision the encrypted immutable connection through CreateSourceConnection on
current managed authority, using protected transport and target-admin credentials.
Supply the external TLS settings, physical database and expected system identifier.
The operator mounts the shared 32-byte catalog key into managed/source poolers;
it does not place external database credentials in the Shard. Internal mTLS is
required. Gateway workloads receive no migration key or serving-control token.
Credential rotation, hot reload and connection-reference changes are out of scope.

Sources register a fresh Pod UID identity before protected bootstrap and start
with admission closed. Backend readiness proves external identity/connectivity;
it cannot authorize admission. The migration controller supplies normalized
admission intents and independent gateway routing policy. The operator neither
advances migration operations nor chooses source/target routing.

Source Pods use RestartPolicy Never, a pooler-only container, explicit resources,
no PostgreSQL data PVC, pgctld, backup manager or multiorch membership. A failed
process can be replaced only when its termination is proven. Unknown topology
identities block replacement; node loss or missing Pod/topology records do not
prove termination. Managed consensus IDs remain stable; process-start tokens
allow the controller to distinguish container/process restarts.

Removing or replacing a live source requires ReadSourceLifecycle authorization
from current managed authority. That controller-written projection binds the
owner, source reference/configuration and completed CLOSED intent. Routing,
readiness and a missing intent never authorize deletion. Deletion requests
SIGTERM, then the Pod finalizer waits for actual Kubernetes container-termination
evidence. The operator publishes/repairs SHUTDOWN proof for the exact Pod UID
before releasing the finalizer, retaining proof during authority outages.

Topology proof garbage collection requires a separate durable authorization
listing each exact process ID. The controller grants this only after termination
is proven and no recoverable journal operation references the process. Retirement
permission alone does not release proof. Unreachable or unproven node-death
cases remain blocked; real node-loss acceptance requires real cluster evidence.

Build matching core/operator draft images and deploy their CRDs and workloads
together. The replacement control protocol is a coordinated prototype rollout,
not wire compatibility with the superseded reference stack.

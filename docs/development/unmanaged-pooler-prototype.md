# Unmanaged pooler provisioning prototype

This companion depends on the migration-serving core draft stack. Build the
operator with the pinned fork dependency in `go.mod`, and deploy matching core
images (ordinary builds, without the `migration_demo` tag). Source pods run only
multipooler; they have no pgctld, PostgreSQL data PVC, backup sidecar, or managed
consensus readiness gate.

## Provisioning sequence

1. Enable the cluster's internal mTLS. Existing topology cells must include every
   source cell. Deploy the core stack into the managed cohort and gateways.
2. Generate a random 32-byte migration key outside the CR and store it in a
   namespaced Kubernetes Secret. Store its SHA256 digest, encoded as 64 hex
   characters, in a *different* Secret for gateways. Gateways never mount the
   encryption key. Treat both as credentials and restrict Secret RBAC.
3. Patch the managed Shard's `migrationKeySecretRef` with a dedicated field
   manager. Wait for the managed poolers to pick up the key. Patch the Cell's
   `servingControlTokenSecretRef`, then wait for gateway rollout. The parent
   controllers omit these fields from their server-side apply payloads; migration
   tooling owns them independently.
4. Port-forward the gateway Deployment's private admin port 5434. Authenticate
   with a target administrator and execute `CREATE CONNECTION ... REQUEST ID ...`.
   Endpoint/password/TLS settings go to the encrypted target catalog, never to
   the Shard spec, Pod arguments or environment.
5. Add `Shard.spec.unmanagedPoolers`, then wait for source backend preparation
   through the core health interface before `ATTACH CONNECTION ... REQUEST ID ...`.
   Kubernetes process readiness alone does not prove backend identity or serving
   admission. The core attach operation verifies every registered source.

Example Shard fields (apply using a migration-specific field manager):

```yaml
spec:
  migrationKeySecretRef:
    name: migration-catalog-key
    key: key
  unmanagedPoolers:
    source:
      connectionName: source
      cells: [zone-a, zone-b]
      replicasPerCell: 1
      connectionBudget: 64
      multipooler:
        resources:
          requests:
            cpu: 250m
            memory: 256Mi
          limits:
            memory: 512Mi
```

Example Cell field:

```yaml
spec:
  servingControlTokenSecretRef:
    name: migration-gateway-token
    key: token
```

The gateway's public Service does not expose 5434. Provision a restricted admin
Service/NetworkPolicy explicitly if port-forwarding is unsuitable.

## Identity, capacity and lifecycle

The source service ID is `k8s-<Pod UID>`. Pods use `restartPolicy: Never`: replacing
a failed process always allocates a fresh Pod UID and topology identity. Process
identity is independent of the stable workload name, preventing a delayed Fence
acknowledgment from being confused with a replacement.

`connectionBudget` divides application pool capacity across all source cells and
replicas. Two admin connections per process are configured separately. This is
an application pool budget; account separately for dedicated notification or
replication connections and unrelated direct source clients. It does not enforce
a database-wide connection quota. New capacity waits while old replicas/specs
await shutdown, avoiding transient multiplication during replacements.

The core migration completion record gates removal. MANAGED alone does not
permit decommissioning. Spec/image/resource changes wait for FENCED or completion.
If the managed authority is unavailable, removal waits. A source Pod finalizer
keeps its topology record until Kubernetes reports the never-restarting container
terminated. A deletion timestamp, missing Pod or unhealthy node is insufficient.
An orphan `k8s-` source identity blocks replacement until shutdown is proven;
normal stale managed-pooler pruning explicitly skips unmanaged records.

After the controller records completion, remove the unmanaged entry. The operator
requests SIGTERM, waits for process termination, unregisters its unique topology
record, then removes the Pod finalizer. The external PostgreSQL instance remains
untouched. Completed detach and controller cleanup are independent core actions.

## Prototype limits and validation

Unit tests cover distinct manifests, derived-token mounts, capacity division,
FENCED/completion checks, unavailable authority, terminating-container proof,
orphan replacement refusal, and preservation of source topology records. This
companion has not been demonstrated in a Kubernetes cluster. Node-loss recovery,
Secret rotation/rollout, source health/status presentation and parent server-side
apply ownership need cluster acceptance tests before production use.

The local process E2E and manual demonstration live in the core stack's
`docs/migration-serving-demo.md`; they exclude real migration controllers and
data replication. This operator does not implement replication barriers, serving
mode writes or source database role revocation.

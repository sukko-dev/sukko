# Migration Guide: AUTH_MODE=disabled Removal

## What Changed

`AUTH_MODE=disabled` has been removed. Admin and tenant JWT authentication is now always enforced. The `AUTH_MODE` environment variable still accepts `required` (the default) for backward compatibility, but `disabled` is no longer a valid value.

`DEFAULT_TENANT_ID` has also been removed. All connections must authenticate and provide tenant context via JWT or API key.

## Why

- `AUTH_MODE=disabled` silently removed all authorization, creating a security footgun
- `sukko init` auto-generates admin keypairs, eliminating the original "zero-setup" justification
- The conditional auth logic added cognitive load to every new admin endpoint and spec

## Who Is Affected

**Deployments running `AUTH_MODE=required` (the default)**: No impact. The refactor is transparent.

**Deployments running `AUTH_MODE=disabled`**: Startup will fail with a clear error:

```
AUTH_MODE=disabled has been removed. See docs/migration/auth-mode-removal.md.
Set up admin + tenant JWT auth and remove AUTH_MODE/DEFAULT_TENANT_ID env vars.
```

## Migration Steps

### For CLI users (`sukko` CLI)

Run `sukko init` — it handles keypair generation, admin key registration, and tenant setup automatically. Then `sukko up`.

### For manual / Helm / K8s deployments

1. **Generate an admin keypair**:
   ```bash
   sukko auth keygen
   ```
   Or generate an Ed25519 keypair manually and encode the public key in PEM format.

2. **Set `ADMIN_BOOTSTRAP_KEY`** on the provisioning service:
   ```bash
   ADMIN_BOOTSTRAP_KEY="<base64-encoded-public-key>"
   ```
   Provisioning registers this key on first startup.

3. **Create at least one tenant** via the provisioning API:
   ```bash
   curl -X POST http://provisioning:8080/api/v1/tenants \
     -H "Authorization: Bearer <admin-jwt>" \
     -H "Content-Type: application/json" \
     -d '{"tenant_id": "my-tenant", "name": "My Tenant"}'
   ```

4. **Register a tenant signing key** and issue tenant JWTs for clients.

5. **Remove deprecated env vars**:
   - Remove `AUTH_MODE=disabled`
   - Remove `DEFAULT_TENANT_ID`
   - `AUTH_MODE=required` can be left (it's the default) or removed entirely.

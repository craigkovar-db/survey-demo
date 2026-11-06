"""On-behalf-of-user (OBO) auth helpers for the webapp.

When the app runs on Databricks Apps with user authorization enabled, each
request arrives with a short-lived user access token in the
`X-Forwarded-Access-Token` header. Using that token (instead of the app's
service-principal credentials) means every downstream call — Genie One,
Lakebase credential minting, serving endpoints, SQL warehouse — is authorized
as the END USER, so their UC permissions govern what they can see.

Local dev has no such header; in that case these helpers return None and the
callers fall back to their default WorkspaceClient() (profile auth), keeping
the dev loop unchanged.

See https://docs.databricks.com/dev-tools/databricks-apps/auth for the full
pattern and the `user_api_scopes` declaration the platform needs in app.yaml.
"""
from __future__ import annotations

import os

from databricks.sdk import WorkspaceClient
from fastapi import Request

OBO_HEADER = "x-forwarded-access-token"


def user_token_from(request: Request) -> str | None:
    """Return the per-request OBO token, or None when running locally."""
    return request.headers.get(OBO_HEADER)


def workspace_for(user_token: str | None) -> WorkspaceClient:
    """Return a WorkspaceClient scoped to the end user when a token is present,
    otherwise fall back to the app's default credentials (profile locally,
    service-principal on Databricks Apps when OBO is unavailable).

    User-scoped clients are NOT cached — each request belongs to a different
    user, and sharing the client across users would leak permissions.

    Why `auth_type="pat"` on the OBO path:
        On Databricks Apps the environment is already populated with the
        app's service-principal OAuth creds (DATABRICKS_CLIENT_ID /
        DATABRICKS_CLIENT_SECRET). If we just pass `token=user_token` the
        SDK auto-detects both oauth AND pat and refuses with:
            "more than one authorization method configured: oauth and pat"
        Setting `auth_type="pat"` forces the SDK to use ONLY the provided
        token and ignore the env-var OAuth config.
    """
    if user_token:
        host = (os.environ.get("DATABRICKS_HOST") or "").rstrip("/")
        return WorkspaceClient(host=host, token=user_token, auth_type="pat")
    return WorkspaceClient()

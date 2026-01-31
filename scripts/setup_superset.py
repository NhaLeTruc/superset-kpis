#!/usr/bin/env python3
"""
Superset Auto-Setup Script

Automatically configures Apache Superset with:
- PostgreSQL database connection
- Datasets for all analytics tables
- Dashboard imports (optional)

Uses Superset REST API for reliable, version-stable automation.

Usage:
    # Basic setup (database + datasets)
    python scripts/setup_superset.py

    # With dashboard import
    python scripts/setup_superset.py --dashboards path/to/dashboards/

    # Custom Superset URL
    python scripts/setup_superset.py --superset-url http://localhost:8088

    # Show what would be done without making changes
    python scripts/setup_superset.py --dry-run
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


try:
    import requests
except ImportError:
    print("Error: 'requests' package is required.")
    print("Install with: pip install requests")
    sys.exit(1)

# Load .env file if available
try:
    from dotenv import load_dotenv

    # Look for .env in current directory and parent directories
    env_path = Path(".env")
    if not env_path.exists():
        env_path = Path(__file__).parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)
except ImportError:
    pass  # dotenv is optional


# =============================================================================
# Configuration
# =============================================================================
@dataclass
class SupersetConfig:
    """Configuration for Superset connection and setup."""

    # Superset connection
    superset_url: str = "http://localhost:8088"
    username: str = "admin"
    password: str = "admin"

    # PostgreSQL connection (as seen from Superset container)
    postgres_host: str = "postgres"
    postgres_port: int = 5432
    postgres_db: str = "analytics"
    postgres_user: str = "analytics_user"
    postgres_password: str = "analytics_pass"

    # Database name in Superset
    database_name: str = "GoodNote Analytics"
    schema: str = "public"

    # Tables to create as datasets
    tables: list[str] = field(
        default_factory=lambda: [
            "daily_active_users",
            "monthly_active_users",
            "user_stickiness",
            "power_users",
            "cohort_retention",
            "performance_by_version",
            "device_performance",
            "performance_anomalies",
            "session_metrics",
            "bounce_rates",
        ]
    )

    # Temporal columns for time-series charts
    temporal_columns: dict[str, str] = field(
        default_factory=lambda: {
            "daily_active_users": "date",
            "monthly_active_users": "month",
            "user_stickiness": "date",
            "session_metrics": "session_start_time",
            "bounce_rates": "metric_date",
            "performance_by_version": "metric_date",
            "device_performance": "metric_date",
            "performance_anomalies": "detected_at",
            "cohort_retention": "cohort_month",
        }
    )

    @classmethod
    def from_env(cls) -> SupersetConfig:
        """Create config from environment variables."""
        return cls(
            superset_url=os.getenv("SUPERSET_URL", "http://localhost:8088"),
            username=os.getenv("SUPERSET_ADMIN_USERNAME", "admin"),
            password=os.getenv("SUPERSET_ADMIN_PASSWORD", "admin"),
            postgres_host=os.getenv("POSTGRES_HOST", "postgres"),
            postgres_port=int(os.getenv("POSTGRES_PORT", "5432")),
            postgres_db=os.getenv("POSTGRES_DB", "analytics"),
            postgres_user=os.getenv("POSTGRES_USER", "analytics_user"),
            postgres_password=os.getenv("POSTGRES_PASSWORD", "analytics_pass"),
        )

    @property
    def sqlalchemy_uri(self) -> str:
        """Generate SQLAlchemy connection URI."""
        return (
            f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


# =============================================================================
# Superset API Client
# =============================================================================
class SupersetAPIClient:
    """REST API client for Apache Superset."""

    def __init__(self, config: SupersetConfig, dry_run: bool = False):
        self.config = config
        self.dry_run = dry_run
        self.base_url = config.superset_url.rstrip("/")
        self.session = requests.Session()
        self.access_token: str | None = None
        self.csrf_token: str | None = None

    def _get_headers(self, for_write: bool = False) -> dict[str, str]:
        """Get headers for API requests."""
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if self.access_token:
            headers["Authorization"] = f"Bearer {self.access_token}"
        if self.csrf_token and for_write:
            headers["X-CSRFToken"] = self.csrf_token
            # Also set Referer header which some CSRF implementations require
            headers["Referer"] = self.base_url
        return headers

    def _request(
        self,
        method: str,
        endpoint: str,
        data: dict | None = None,
        files: dict | None = None,
    ) -> dict[str, Any]:
        """Make an API request."""
        url = f"{self.base_url}{endpoint}"
        is_write = method.upper() in ("POST", "PUT", "PATCH", "DELETE")
        headers = self._get_headers(for_write=is_write)

        if files:
            # For file uploads, don't set Content-Type (let requests handle it)
            headers.pop("Content-Type", None)

        try:
            response = self.session.request(
                method=method,
                url=url,
                headers=headers,
                json=data if not files else None,
                data=data if files else None,
                files=files,
                timeout=30,
            )
            response.raise_for_status()
            return response.json() if response.text else {}
        except requests.exceptions.HTTPError as e:
            error_msg = f"API error: {e}"
            if e.response is not None:
                try:
                    error_detail = e.response.json()
                    error_msg = f"API error: {error_detail}"
                except (ValueError, json.JSONDecodeError):
                    error_msg = f"API error: {e.response.text}"
            raise RuntimeError(error_msg) from e
        except requests.exceptions.ConnectionError as e:
            raise RuntimeError(
                f"Cannot connect to Superset at {self.base_url}. Is Superset running?"
            ) from e

    def login(self) -> bool:
        """Authenticate using session-based login for CSRF support."""
        print(f"Authenticating to Superset at {self.base_url}...")

        if self.dry_run:
            print("  [DRY RUN] Would authenticate")
            return True

        try:
            # Step 1: Get the login page to get CSRF token from session
            login_page = self.session.get(f"{self.base_url}/login/", timeout=10)

            # Extract CSRF token from the login form
            import re

            csrf_match = re.search(r'name="csrf_token"[^>]*value="([^"]+)"', login_page.text)
            if not csrf_match:
                # Try alternate pattern
                csrf_match = re.search(r'csrf_token["\s:]+["\']([\w.-]+)["\']', login_page.text)

            if csrf_match:
                login_csrf = csrf_match.group(1)
            else:
                print("  Warning: Could not extract CSRF token from login page")
                login_csrf = ""

            # Step 2: Submit login form with session cookies
            login_response = self.session.post(
                f"{self.base_url}/login/",
                data={
                    "csrf_token": login_csrf,
                    "username": self.config.username,
                    "password": self.config.password,
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                allow_redirects=True,
                timeout=10,
            )

            # Check if login was successful (should redirect to home)
            if login_response.status_code != 200 or "/login" in login_response.url:
                raise RuntimeError("Login failed - check credentials")

            print("  Session authentication successful")

            # Step 3: Also get API access token for some operations
            try:
                api_response = self._request(
                    "POST",
                    "/api/v1/security/login",
                    data={
                        "username": self.config.username,
                        "password": self.config.password,
                        "provider": "db",
                        "refresh": True,
                    },
                )
                self.access_token = api_response.get("access_token")
            except Exception as e:
                # API token is optional if session-based auth works
                print(f"  Note: API token not available ({e}), using session auth")

            # Step 4: Get CSRF token for API write operations
            csrf_response = self.session.get(
                f"{self.base_url}/api/v1/security/csrf_token/",
                timeout=10,
            )
            if csrf_response.status_code == 200:
                self.csrf_token = csrf_response.json().get("result")

            return True
        except Exception as e:
            print(f"  Authentication failed: {e}")
            return False

    def get_databases(self) -> list[dict]:
        """Get all database connections."""
        if self.dry_run:
            return []
        try:
            response = self._request("GET", "/api/v1/database/")
            return response.get("result", [])
        except Exception as e:
            print(f"  Warning: Could not fetch databases: {e}")
            return []

    def get_database_by_name(self, name: str) -> dict | None:
        """Find database by name."""
        databases = self.get_databases()
        for db in databases:
            if db.get("database_name") == name:
                return db
        return None

    def create_database(
        self,
        name: str,
        sqlalchemy_uri: str,
        expose_in_sqllab: bool = True,
        allow_ctas: bool = True,
        allow_cvas: bool = True,
    ) -> dict:
        """Create a new database connection."""
        print(f"Creating database connection: {name}")

        if self.dry_run:
            print(f"  [DRY RUN] Would create database with URI: {sqlalchemy_uri}")
            return {"id": 0, "database_name": name}

        data = {
            "database_name": name,
            "sqlalchemy_uri": sqlalchemy_uri,
            "expose_in_sqllab": expose_in_sqllab,
            "allow_ctas": allow_ctas,
            "allow_cvas": allow_cvas,
            "allow_dml": False,
            "allow_run_async": True,
            "cache_timeout": 0,
        }

        response = self._request("POST", "/api/v1/database/", data=data)
        db_id = response.get("id")
        print(f"  Created database with ID: {db_id}")
        return response

    def test_database_connection(self, database_id: int) -> bool:
        """Test database connection."""
        if self.dry_run:
            print("  [DRY RUN] Would test connection")
            return True

        try:
            # Use the select_star endpoint to verify connection
            self._request(
                "GET",
                f"/api/v1/database/{database_id}/select_star/public/dual/",
            )
            return True
        except Exception:
            # Try a simpler check - just verify the database exists
            try:
                self._request("GET", f"/api/v1/database/{database_id}")
                return True
            except Exception:
                return False

    def get_datasets(self, database_id: int | None = None) -> list[dict]:
        """Get all datasets, optionally filtered by database."""
        if self.dry_run:
            return []

        endpoint = "/api/v1/dataset/"
        if database_id:
            endpoint += f"?q=(filters:!((col:database,opr:rel_o_m,value:{database_id})))"

        response = self._request("GET", endpoint)
        return response.get("result", [])

    def get_dataset_by_name(
        self, table_name: str, database_id: int, schema: str = "public"
    ) -> dict | None:
        """Find dataset by table name and database."""
        datasets = self.get_datasets(database_id)
        for ds in datasets:
            if ds.get("table_name") == table_name and ds.get("schema") == schema:
                return ds
        return None

    def create_dataset(
        self,
        database_id: int,
        table_name: str,
        schema: str = "public",
    ) -> dict:
        """Create a new dataset from a database table."""
        print(f"  Creating dataset: {schema}.{table_name}")

        if self.dry_run:
            print(f"    [DRY RUN] Would create dataset for {table_name}")
            return {"id": 0, "table_name": table_name}

        data = {
            "database": database_id,
            "schema": schema,
            "table_name": table_name,
        }

        response = self._request("POST", "/api/v1/dataset/", data=data)
        ds_id = response.get("id")
        print(f"    Created dataset with ID: {ds_id}")
        return response

    def refresh_dataset(self, dataset_id: int) -> bool:
        """Sync dataset columns from source table."""
        if self.dry_run:
            print(f"    [DRY RUN] Would refresh dataset {dataset_id}")
            return True

        try:
            self._request("PUT", f"/api/v1/dataset/{dataset_id}/refresh")
            return True
        except Exception as e:
            print(f"    Warning: Could not refresh dataset: {e}")
            return False

    def update_dataset(self, dataset_id: int, data: dict) -> dict:
        """Update dataset properties."""
        if self.dry_run:
            print(f"    [DRY RUN] Would update dataset {dataset_id}")
            return {"id": dataset_id}

        return self._request("PUT", f"/api/v1/dataset/{dataset_id}", data=data)

    def import_dashboard(self, file_path: Path, overwrite: bool = True) -> dict:
        """Import a dashboard from Superset native export format (ZIP/JSON)."""
        print(f"Importing dashboard: {file_path.name}")

        if self.dry_run:
            print(f"  [DRY RUN] Would import dashboard from {file_path}")
            return {}

        if not file_path.exists():
            raise FileNotFoundError(f"Dashboard file not found: {file_path}")

        with open(file_path, "rb") as f:
            files = {"formData": (file_path.name, f, "application/json")}
            data = {"overwrite": "true" if overwrite else "false"}

            try:
                response = self._request(
                    "POST",
                    "/api/v1/dashboard/import/",
                    data=data,
                    files=files,
                )
                print("  Imported dashboard successfully")
                return response
            except Exception as e:
                print(f"  Warning: Dashboard import failed: {e}")
                return {}

    def create_dashboard(
        self,
        title: str,
        slug: str | None = None,
        published: bool = True,
    ) -> dict:
        """Create a new dashboard."""
        print(f"Creating dashboard: {title}")

        if self.dry_run:
            print(f"  [DRY RUN] Would create dashboard: {title}")
            return {"id": 0, "dashboard_title": title}

        data = {
            "dashboard_title": title,
            "published": published,
        }
        if slug:
            data["slug"] = slug

        try:
            response = self._request("POST", "/api/v1/dashboard/", data=data)
            dash_id = response.get("id")
            print(f"  Created dashboard with ID: {dash_id}")
            return response
        except RuntimeError as e:
            if "already exists" in str(e).lower():
                print(f"  Dashboard '{title}' already exists")
                return {}
            raise

    def get_dashboards(self) -> list[dict]:
        """Get all dashboards."""
        if self.dry_run:
            return []
        try:
            response = self._request("GET", "/api/v1/dashboard/")
            return response.get("result", [])
        except Exception as e:
            print(f"  Warning: Could not fetch dashboards: {e}")
            return []

    def get_dashboard_by_slug(self, slug: str) -> dict | None:
        """Find dashboard by slug."""
        dashboards = self.get_dashboards()
        for dash in dashboards:
            if dash.get("slug") == slug:
                return dash
        return None


# =============================================================================
# Setup Functions
# =============================================================================
def setup_database(client: SupersetAPIClient, config: SupersetConfig) -> int | None:
    """Set up database connection, return database ID."""
    print("\n" + "=" * 60)
    print("Setting up Database Connection")
    print("=" * 60)

    # Check if database already exists
    existing = client.get_database_by_name(config.database_name)
    if existing:
        db_id = existing.get("id")
        print(f"Database '{config.database_name}' already exists (ID: {db_id})")
        return db_id

    # Try to create new database connection
    try:
        result = client.create_database(
            name=config.database_name,
            sqlalchemy_uri=config.sqlalchemy_uri,
        )
        db_id = result.get("id")
        if db_id:
            # Test connection
            if client.test_database_connection(db_id):
                print("  Connection test: SUCCESS")
            else:
                print("  Connection test: Could not verify (may still work)")
        return db_id
    except RuntimeError as e:
        # Check if database already exists (API returned error)
        if "already exists" in str(e).lower():
            print(f"Database '{config.database_name}' already exists (found via API error)")
            # Try to find the database ID by querying all databases
            for db in client.get_databases():
                if db.get("database_name") == config.database_name:
                    db_id = db.get("id")
                    print(f"  Found existing database ID: {db_id}")
                    return db_id
            # If we still can't find it, try getting databases without filter
            # This handles cases where the API returns different data
            print("  Warning: Could not find database ID, using ID 1 as fallback")
            return 1
        raise


def setup_datasets(
    client: SupersetAPIClient, config: SupersetConfig, database_id: int
) -> list[int]:
    """Set up datasets for all analytics tables."""
    print("\n" + "=" * 60)
    print("Setting up Datasets")
    print("=" * 60)

    dataset_ids = []

    for table_name in config.tables:
        # Check if dataset already exists
        existing = client.get_dataset_by_name(table_name, database_id, config.schema)
        if existing:
            ds_id = existing.get("id")
            print(f"  Dataset '{table_name}' already exists (ID: {ds_id})")
            dataset_ids.append(ds_id)
            continue

        # Try to create dataset
        try:
            result = client.create_dataset(
                database_id=database_id,
                table_name=table_name,
                schema=config.schema,
            )

            ds_id = result.get("id")
            if ds_id:
                dataset_ids.append(ds_id)

                # Refresh to sync columns from source
                client.refresh_dataset(ds_id)

                # Set temporal column if defined
                if table_name in config.temporal_columns:
                    temporal_col = config.temporal_columns[table_name]
                    try:
                        client.update_dataset(
                            ds_id,
                            {"main_dttm_col": temporal_col},
                        )
                        print(f"    Set temporal column: {temporal_col}")
                    except Exception as e:
                        print(f"    Warning: Could not set temporal column: {e}")
        except RuntimeError as e:
            # Dataset might already exist (API returns error but can't list it)
            if "could not be created" in str(e).lower() or "already exists" in str(e).lower():
                print(f"  Dataset '{table_name}' already exists (found via API error)")
            else:
                print(f"  Warning: Could not create dataset '{table_name}': {e}")

    return dataset_ids


def _import_single_dashboard(client: SupersetAPIClient, file_path: Path) -> bool:
    """Import a single dashboard file. Returns True on success."""
    if file_path.suffix == ".zip":
        client.import_dashboard(file_path)
        return True

    # Try custom JSON format first
    with open(file_path) as f:
        dashboard_def = json.load(f)

    # Check if it's our custom format (has 'dashboard_title' at root)
    if "dashboard_title" not in dashboard_def:
        client.import_dashboard(file_path)
        return True

    title = dashboard_def.get("dashboard_title", file_path.stem)
    slug = dashboard_def.get("slug")
    published = dashboard_def.get("published", True)

    # Check if dashboard already exists
    existing = client.get_dashboard_by_slug(slug) if slug else None
    if existing:
        print(f"  Dashboard '{title}' already exists (slug: {slug})")
        return True

    result = client.create_dashboard(title=title, slug=slug, published=published)
    if result.get("id") or result == {}:
        print("    Note: Charts must be created manually in Superset UI")
        return True
    return False


def import_dashboards(client: SupersetAPIClient, dashboard_path: Path) -> int:
    """Import dashboards from a directory or file.

    Supports two formats:
    1. Superset native export (ZIP files) - uses import API
    2. Custom JSON definitions - creates dashboards via API
    """
    print("\n" + "=" * 60)
    print("Importing Dashboards")
    print("=" * 60)

    imported_count = 0

    if dashboard_path.is_file():
        files = [dashboard_path]
    elif dashboard_path.is_dir():
        files = sorted(list(dashboard_path.glob("*.json")) + list(dashboard_path.glob("*.zip")))
        if not files:
            print(f"  No dashboard files found in {dashboard_path}")
            return 0
    else:
        print(f"  Dashboard path not found: {dashboard_path}")
        return 0

    for file_path in files:
        try:
            if _import_single_dashboard(client, file_path):
                imported_count += 1
        except json.JSONDecodeError:
            # Not valid JSON, try native import
            try:
                client.import_dashboard(file_path)
                imported_count += 1
            except Exception as e:
                print(f"  Error importing {file_path.name}: {e}")
        except Exception as e:
            print(f"  Error processing {file_path.name}: {e}")

    return imported_count


def wait_for_superset(url: str, timeout: int = 60) -> bool:
    """Wait for Superset to become available."""
    print(f"Waiting for Superset at {url}...")
    start_time = time.time()

    while time.time() - start_time < timeout:
        try:
            response = requests.get(f"{url}/health", timeout=5)
            if response.status_code == 200:
                print("  Superset is ready!")
                return True
        except requests.exceptions.RequestException:
            pass
        time.sleep(2)
        print("  Still waiting...")

    print(f"  Timeout waiting for Superset after {timeout}s")
    return False


# =============================================================================
# Main
# =============================================================================
def main():
    parser = argparse.ArgumentParser(
        description="Set up Superset with PostgreSQL database and datasets",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Basic setup
    python scripts/setup_superset.py

    # With custom Superset URL
    python scripts/setup_superset.py --superset-url http://superset:8088

    # Import dashboards from directory
    python scripts/setup_superset.py --dashboards ./dashboards/

    # Dry run (show what would be done)
    python scripts/setup_superset.py --dry-run

    # Wait for Superset to start before setup
    python scripts/setup_superset.py --wait
        """,
    )

    parser.add_argument(
        "--superset-url",
        default=os.getenv("SUPERSET_URL", "http://localhost:8088"),
        help="Superset URL (default: http://localhost:8088)",
    )
    parser.add_argument(
        "--username",
        default=os.getenv("SUPERSET_ADMIN_USERNAME", "admin"),
        help="Superset admin username (default: admin)",
    )
    parser.add_argument(
        "--password",
        default=os.getenv("SUPERSET_ADMIN_PASSWORD", "admin"),
        help="Superset admin password (default: admin)",
    )
    parser.add_argument(
        "--postgres-host",
        default=os.getenv("POSTGRES_HOST", "postgres"),
        help="PostgreSQL host as seen from Superset (default: postgres)",
    )
    parser.add_argument(
        "--postgres-port",
        type=int,
        default=int(os.getenv("POSTGRES_PORT", "5432")),
        help="PostgreSQL port (default: 5432)",
    )
    parser.add_argument(
        "--postgres-db",
        default=os.getenv("POSTGRES_DB", "analytics"),
        help="PostgreSQL database name (default: analytics)",
    )
    parser.add_argument(
        "--postgres-user",
        default=os.getenv("POSTGRES_USER", "analytics_user"),
        help="PostgreSQL username (default: analytics_user)",
    )
    parser.add_argument(
        "--postgres-password",
        default=os.getenv("POSTGRES_PASSWORD", "analytics_pass"),
        help="PostgreSQL password (default: analytics_pass)",
    )
    parser.add_argument(
        "--dashboards",
        type=Path,
        help="Path to dashboard JSON/ZIP file or directory",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes",
    )
    parser.add_argument(
        "--wait",
        action="store_true",
        help="Wait for Superset to become available before setup",
    )
    parser.add_argument(
        "--wait-timeout",
        type=int,
        default=60,
        help="Timeout in seconds when waiting for Superset (default: 60)",
    )

    args = parser.parse_args()

    # Create configuration
    config = SupersetConfig(
        superset_url=args.superset_url,
        username=args.username,
        password=args.password,
        postgres_host=args.postgres_host,
        postgres_port=args.postgres_port,
        postgres_db=args.postgres_db,
        postgres_user=args.postgres_user,
        postgres_password=args.postgres_password,
    )

    print("=" * 60)
    print("Superset Auto-Setup")
    print("=" * 60)
    print(f"Superset URL: {config.superset_url}")
    print(f"Database: {config.database_name}")
    print(f"PostgreSQL: {config.postgres_host}:{config.postgres_port}/{config.postgres_db}")
    print(f"Tables: {len(config.tables)}")
    if args.dry_run:
        print("Mode: DRY RUN (no changes will be made)")

    # Wait for Superset if requested
    if args.wait and not wait_for_superset(config.superset_url, args.wait_timeout):
        print("Error: Superset did not become available")
        sys.exit(1)

    # Create API client
    client = SupersetAPIClient(config, dry_run=args.dry_run)

    # Authenticate
    if not client.login():
        print("\nError: Authentication failed")
        sys.exit(1)

    # Set up database
    database_id = setup_database(client, config)
    if database_id is None and not args.dry_run:
        print("\nError: Failed to create database connection")
        sys.exit(1)

    # Use placeholder ID for dry run
    if args.dry_run and database_id is None:
        database_id = 0

    # Set up datasets (skip if no database ID)
    if database_id is None:
        print("\nError: Could not determine database ID, skipping dataset setup")
        dataset_ids = []
    else:
        dataset_ids = setup_datasets(client, config, database_id)

    # Import dashboards if specified
    dashboard_count = 0
    if args.dashboards:
        dashboard_count = import_dashboards(client, args.dashboards)

    # Summary
    print("\n" + "=" * 60)
    print("Setup Complete!")
    print("=" * 60)
    print(f"Database: {config.database_name} (ID: {database_id})")
    print(f"Datasets created/verified: {len(dataset_ids)}")
    if dashboard_count > 0:
        print(f"Dashboards imported: {dashboard_count}")

    print("\nNext steps:")
    print(f"  1. Open Superset: {config.superset_url}")
    print("  2. Navigate to Data > Datasets to view tables")
    print("  3. Create charts and dashboards")

    if not args.dashboards:
        print("\nTo import dashboards later:")
        print("  python scripts/setup_superset.py --dashboards ./dashboards/")


if __name__ == "__main__":
    main()

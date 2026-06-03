#!/usr/bin/env python3
"""SP-PG-ORM-DJANGO — Django ORM integration validation smoke.

Runs a REAL Django ORM workload (models + schema_editor DDL + ORM CRUD)
end-to-end against KesselDB's PG-wire gateway. This is the SECOND dominant
Python ORM (after SQLAlchemy, which now passes full declarative-ORM CRUD).
Django is STRICTER than SQLAlchemy: it issues Postgres-specific
introspection on connect, emits SERIAL/constraint DDL, wraps writes in
SAVEPOINTs, and uses qualified RETURNING.

Triage harness: every stage runs in its own try/except so a failure in one
stage does not mask later stages. Each stage prints
`STAGE <name>: PASS|FAIL <detail>` so the gateway log + this stdout pin the
EXACT boundary of Django ORM support.

Connection: postgresql://test:admin@127.0.0.1:5545/kesseldb (port 5545 —
sibling SQL-DML agent owns 5546/6546; this arc owns 5545/6545).
"""
import os
import sys
import traceback

import django
from django.conf import settings

PORT = "5545"

# --- materialize a real on-disk Django app package `smokeapp` ---------------
# Django 6's app registry requires the app + its models module to be a real
# importable package (synthetic ModuleType objects trip __spec__/path checks).
# We write a minimal package to a scratch dir and put it on sys.path.
_PKG_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".django_smoke_pkg")
_APP_DIR = os.path.join(_PKG_ROOT, "smokeapp")
os.makedirs(_APP_DIR, exist_ok=True)
open(os.path.join(_APP_DIR, "__init__.py"), "w").close()
with open(os.path.join(_APP_DIR, "models.py"), "w") as _f:
    _f.write(
        "from django.db import models\n\n\n"
        "class Author(models.Model):\n"
        "    name = models.CharField(max_length=32)\n\n"
        "    class Meta:\n"
        "        app_label = 'smokeapp'\n"
    )
if _PKG_ROOT not in sys.path:
    sys.path.insert(0, _PKG_ROOT)

settings.configure(
    DEBUG=True,
    DATABASES={
        "default": {
            "ENGINE": "django.db.backends.postgresql",
            "NAME": "kesseldb",
            "USER": "test",
            "PASSWORD": "admin",
            "HOST": "127.0.0.1",
            "PORT": PORT,
            # Django wraps each request/atomic block in a transaction by
            # default; AUTOCOMMIT True keeps individual ORM calls outside an
            # explicit BEGIN unless we open one. Leave defaults to observe
            # Django's real behaviour against KesselDB.
        }
    },
    INSTALLED_APPS=["smokeapp"],
    USE_TZ=False,
    DEFAULT_AUTO_FIELD="django.db.models.AutoField",
)

django.setup()

from smokeapp.models import Author  # noqa: E402

results = []  # (stage, ok, detail)


def stage(name, fn):
    try:
        detail = fn()
        results.append((name, True, detail))
        print(f"STAGE {name}: PASS {detail if detail else ''}".rstrip())
        return True
    except Exception as e:  # noqa: BLE001 — triage harness wants every error
        msg = f"{type(e).__name__}: {e}"
        results.append((name, False, msg))
        print(f"STAGE {name}: FAIL {msg}")
        tb = traceback.format_exc().strip().splitlines()
        if tb:
            print(f"    {tb[-1]}")
        return False


def main():
    from django.db import connection

    print(f"# Django {django.get_version()} -> postgresql://test:admin@127.0.0.1:{PORT}/kesseldb")

    # --- 1. connect probe ---
    def _connect():
        with connection.cursor() as c:
            c.execute("SELECT 1")
            c.fetchone()
        return "connection.cursor() + SELECT 1 OK"

    stage("connect", _connect)

    # --- 2. schema create via Django's schema editor (real DDL) ---
    # Django emits: CREATE TABLE "smokeapp_author" ("id" serial NOT NULL
    # PRIMARY KEY, "name" varchar(32) NOT NULL)
    def _create():
        with connection.schema_editor() as editor:
            editor.create_model(Author)
        return "schema_editor.create_model(Author)"

    create_ok = stage("schema_create", _create)

    # Fallback: if Django's DDL shape was rejected, create the table
    # out-of-band so the downstream ORM CRUD stages can still be measured
    # against the live gateway independent of the DDL boundary.
    if not create_ok:
        # Use UNQUOTED, BIGSERIAL-spelled DDL — the shape KesselDB's SQL
        # surface DOES accept — so the table exists and the downstream ORM
        # CRUD stages still run against a live table. This isolates whether
        # the boundary is purely Django's identifier-quoting (the ORM
        # INSERT/SELECT/UPDATE/DELETE also quote, so they too will surface
        # the quote gap) vs. a deeper engine/CRUD gap.
        def _create_raw():
            with connection.cursor() as c:
                c.execute(
                    "CREATE TABLE smokeapp_author "
                    "(id BIGSERIAL PRIMARY KEY, name VARCHAR(32) NOT NULL)"
                )
            return "raw CREATE TABLE fallback (unquoted BIGSERIAL)"

        stage("schema_create_raw_fallback", _create_raw)

    table_exists = any(
        r[1] for r in results
        if r[0] in ("schema_create", "schema_create_raw_fallback")
    )
    if not table_exists:
        print("# no table — skipping CRUD stages")
        finish()
        return

    # --- 3. ORM INSERT + autoincrement pk (objects.create -> RETURNING id) ---
    pk_holder = {}

    def _insert():
        a = Author.objects.create(name="tolkien")
        pk_holder["pk"] = a.pk
        return f"create(name=tolkien) -> pk={a.pk}"

    insert_ok = stage("orm_insert_autoincrement", _insert)

    # --- 4. ORM SELECT all (values_list) ---
    def _select_all():
        data = sorted(Author.objects.values_list("id", "name"))
        return f"values_list -> {data}"

    stage("orm_select_all", _select_all)

    # --- 5. ORM get by pk (SELECT WHERE id = $1) ---
    def _get():
        if "pk" not in pk_holder:
            raise RuntimeError("no pk from insert stage")
        got = Author.objects.get(pk=pk_holder["pk"])
        return f"get(pk={pk_holder['pk']}) -> ({got.id}, {got.name})"

    stage("orm_get_by_pk", _get)

    # --- 6. ORM filtered UPDATE (UPDATE WHERE id = $1) ---
    def _update():
        if "pk" not in pk_holder:
            raise RuntimeError("no pk from insert stage")
        n = Author.objects.filter(pk=pk_holder["pk"]).update(name="jrr")
        data = sorted(Author.objects.values_list("id", "name"))
        return f"update -> {n} row(s); now {data}"

    stage("orm_update", _update)

    # --- 7. ORM filtered DELETE (DELETE WHERE id = $1) ---
    def _delete():
        if "pk" not in pk_holder:
            raise RuntimeError("no pk from insert stage")
        n, _detail = Author.objects.filter(pk=pk_holder["pk"]).delete()
        cnt = Author.objects.count()
        return f"delete -> {n} row(s); remaining count={cnt}"

    stage("orm_delete", _delete)

    finish()


def finish():
    print("\n=== DJANGO ORM SMOKE SUMMARY ===")
    npass = sum(1 for _, ok, _ in results if ok)
    for name, ok, _detail in results:
        print(f"  {'PASS' if ok else 'FAIL'}  {name}")
    print(f"--- {npass}/{len(results)} stages PASS ---")
    print("DJANGO ORM SMOKE COMPLETE")


if __name__ == "__main__":
    main()
    sys.exit(0)

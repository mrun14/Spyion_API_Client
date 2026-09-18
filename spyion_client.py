# spyion_client.py
"""
See internal Documentation for details
"""
import io
import os
import pyarrow as pa
import requests

_DEFAULT_URL = os.environ.get("SPYION_API_URL", "http://localhost:8060")
_ARROW_MIME = "application/vnd.apache.arrow.stream"


def mf(column, value, conj="AND", type="text"):
    """Build one meta-filter in the app's native grammar.
    conj: 'AND' | 'OR' | 'AND NOT'.  type: 'text' (wildcards via *), 'range'
    (value=(min,max)), or 'date' (value=(start,end))."""
    return {"column": column, "value": value, "conjunction": conj, "type": type}


def _headers(token):
    tok = token or os.environ.get("SPYION_API_TOKEN", "")
    if not tok:
        raise RuntimeError("No token. Set SPYION_API_TOKEN or pass token=.")
    return {"Authorization": f"Bearer {tok}"}


def spyion_extract(data_type, filters=None, columns=None, cycles=None,
                   down_sample=None, dry_run=False, allow_large=False,
                   base_url=None, token=None, timeout=300):
    """Pull harmonized data as a pandas DataFrame.

    data_type   : e.g. 'cycling.timeseries', 'lased.spectrogram', 'lased.psd'
    filters     : list of mf(...) dicts (df_meta grammar; selects CELLS)
    columns     : projection; None = all
    cycles      : cycling only; list of cycle numbers (pushed down)
    down_sample : keep every Nth row within a cycle; None = all (see server note)
    dry_run     : return an estimate dict instead of data (call this first on
                  big pulls — it reads only parquet footers)
    allow_large : override the row cap for a deliberately huge pull
    """
    base = (base_url or _DEFAULT_URL).rstrip("/")
    body = {"data_type": data_type, "filters": filters, "columns": columns,
            "cycles": cycles, "down_sample": down_sample, "allow_large": allow_large}
    body = {k: v for k, v in body.items() if v is not None}

    if dry_run:
        r = requests.post(f"{base}/v1/dry-run", json=body,
                          headers=_headers(token), timeout=timeout)
        r.raise_for_status()
        return r.json()

    r = requests.post(f"{base}/v1/extract", json=body,
                      headers=_headers(token), timeout=timeout)
    if r.status_code >= 400:
        # Surface the server's helpful message (bad column, over-cap, etc.)
        try:
            raise RuntimeError(f"[{r.status_code}] {r.json().get('detail', r.text)}")
        except ValueError:
            r.raise_for_status()
    with pa.ipc.open_stream(io.BytesIO(r.content)) as reader:
        return reader.read_all().to_pandas()


def catalog(base_url=None, token=None, timeout=60):
    """Discover available data_types, filterable columns, and current limits."""
    base = (base_url or _DEFAULT_URL).rstrip("/")
    r = requests.get(f"{base}/v1/catalog", headers=_headers(token), timeout=timeout)
    r.raise_for_status()
    return r.json()


def spyion_metadata(domain, filters=None, columns=None, dry_run=False,
                    base_url=None, token=None, timeout=120):
    """Pull the df_meta catalog rows matching the filter, as a pandas DataFrame.

    Same filter grammar as spyion_extract, but returns the METADATA itself
    (one row per matched cell) instead of per-cell time-series/cycle data.

    domain   : 'cycling' | 'lased' | 'model' | 'beam'
    filters  : list of mf(...) dicts
    columns  : project to these df_meta columns; None = all
    dry_run  : return {rows_matched, columns} instead of the data
    """
    base = (base_url or _DEFAULT_URL).rstrip("/")
    body = {"domain": domain, "filters": filters, "columns": columns, "dry_run": dry_run}
    body = {k: v for k, v in body.items() if v is not None}
    r = requests.post(f"{base}/v1/metadata", json=body,
                      headers=_headers(token), timeout=timeout)
    if r.status_code >= 400:
        try:
            raise RuntimeError(f"[{r.status_code}] {r.json().get('detail', r.text)}")
        except ValueError:
            r.raise_for_status()
    if dry_run:
        return r.json()
    with pa.ipc.open_stream(io.BytesIO(r.content)) as reader:
        return reader.read_all().to_pandas()
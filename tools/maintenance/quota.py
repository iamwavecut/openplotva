"""Safe Coding Plan error classification shared by both inference clients."""
from __future__ import annotations

import math
import time
from email.utils import parsedate_to_datetime


def retry_after(value, now=None):
    """Return a bounded provider delay; missing/invalid values require backoff."""
    if value is None or isinstance(value, bool):
        return None
    try:
        seconds = float(value)
    except (ValueError, TypeError):
        try:
            date = parsedate_to_datetime(value)
            if date.tzinfo is None:
                return None
            seconds = date.timestamp() - (time.time() if now is None else now)
        except (ValueError, TypeError, OverflowError):
            return None
    return max(60, min(604800, math.ceil(seconds))) if math.isfinite(seconds) else None


def limited(status, document=None):
    error = document.get('error', {}) if isinstance(document, dict) else {}
    if not isinstance(error, dict):
        error = {}
    code = str(error.get('code', ''))
    # Expired subscriptions, credentials and permissions require intervention,
    # even when the provider uses HTTP 429 for those account errors.
    if code in {'1000', '1001', '1002', '1003', '1004', '1110', '1111', '1112', '1113',
                '1121', '1220', '1309', '1311', '1313'}:
        return False
    return (status == 429 or code in {'1302', '1303', '1304', '1305', '1308', '1310', '1312'}
            or error.get('type') in {'rate_limit_error', 'rate_limit_exceeded', 'quota_exceeded'})

"""PII masking helpers. Masking keeps the shape of the value (so analysts can
still see 'this is an @example.com address') while removing the identity."""
import pandas as pd


def mask_email(email) -> str | None:
    """pr***@example.com — keep the first 2 chars of the local part and the full domain."""
    if email is None or (isinstance(email, float) and pd.isna(email)) or str(email).strip() == "":
        return None
    email = str(email).strip()
    if "@" not in email:
        return "***"
    local, domain = email.rsplit("@", 1)
    return f"{local[:2]}***@{domain}"


def mask_phone(phone) -> str | None:
    """98******21 — keep the first 2 and last 2 digits; everything else becomes *."""
    if phone is None or (isinstance(phone, float) and pd.isna(phone)) or str(phone).strip() == "":
        return None
    phone = str(phone).strip()
    if len(phone) <= 4:
        return "*" * len(phone)
    return phone[:2] + "*" * (len(phone) - 4) + phone[-2:]


def mask_customers(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["email"] = out["email"].map(mask_email)
    out["phone"] = out["phone"].map(mask_phone)
    return out

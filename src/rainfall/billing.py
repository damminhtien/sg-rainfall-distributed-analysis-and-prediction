"""Validate the fixed Google Cloud project, Education account, and allowed credit identity."""

ALLOWED_PROJECT_ID = "sg-rainfall-dsa5208-261011"
ALLOWED_BILLING_ACCOUNT_ID = "01DAD8-AE5F1A-E3910A"
ALLOWED_CREDIT_ID = "3LU63BPGY77JFDQQ"


def validate_billing_target(
    *, project_id, billing_account_id, billing_account_name, billing_enabled, credit_id
):
    """Fail unless the live project uses the only user-authorized billing identity."""
    if project_id != ALLOWED_PROJECT_ID:
        raise ValueError("Only the authorised rainfall project may be used.")
    if billing_account_id != ALLOWED_BILLING_ACCOUNT_ID:
        raise ValueError("Only the Education billing account is authorised.")
    if billing_account_name != f"billingAccounts/{ALLOWED_BILLING_ACCOUNT_ID}":
        raise ValueError("The project is not linked to the Education billing account.")
    if billing_enabled is not True:
        raise ValueError("Billing must be enabled on the authorised account.")
    if credit_id != ALLOWED_CREDIT_ID:
        raise ValueError("Only credit 3LU63BPGY77JFDQQ is authorised for this work.")
    return {
        "project_id": project_id,
        "billing_account_id": billing_account_id,
        "credit_id": credit_id,
        "billing_enabled": True,
        "credit_application": (
            "Promotional credits apply automatically at billing-account scope; "
            "the CLI cannot pin a credit ID to an individual job."
        ),
    }


def validate_runtime_credit_gate(runtime, *, project_id, billing_info):
    """Validate the immutable run identity and the current live billing linkage."""
    if runtime.get("project_id") != project_id:
        raise ValueError("Runtime manifest is for an unexpected project.")
    if runtime.get("billing_enabled") is not True:
        raise ValueError("Runtime manifest does not record billing enabled.")
    return validate_billing_target(
        project_id=project_id,
        billing_account_id=runtime.get("billing_account_id"),
        billing_account_name=billing_info.get("billingAccountName"),
        billing_enabled=billing_info.get("billingEnabled"),
        credit_id=runtime.get("credit_id"),
    )

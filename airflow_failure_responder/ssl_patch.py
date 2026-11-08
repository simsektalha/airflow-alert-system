import ssl


def apply_insecure_ssl_patch() -> None:
    """Disable SSL verification globally (NOT recommended). Keep for backward compatibility."""
    _original_ctx = ssl.create_default_context

    def patched_ctx(*args, **kwargs):
        ctx = _original_ctx(*args, **kwargs)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx

    ssl.create_default_context = patched_ctx

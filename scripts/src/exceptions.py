# Exceptions for shared ingestion logic

class PermanentIngestionError(Exception):
    # Permanent fails (product doesn't exist, checksum mismatch)
    pass


class TransientIngestionError(Exception):
    # Retryable fails (connection error, 5xx errors)
    # If we run out of retrys, it's logged as "failed_retryable"
    pass
# exceptions.py

class RateLimitExceeded(Exception):
    """Raised when an API rate limit is exceeded."""
    def __init__(self, message="API rate limit exceeded"):
        self.message = message
        super().__init__(self.message)


# Optional: Add more custom exceptions as needed
class CacheError(Exception):
    """Raised when there’s an issue with cache operations."""
    def __init__(self, message="Cache operation failed"):
        self.message = message
        super().__init__(self.message)


class WebDriverNotEnabledError(Exception):
    """Raised when WebDriver is required but not enabled."""
    def __init__(self, message="WebDriver is not enabled for this operation"):
        self.message = message
        super().__init__(self.message)
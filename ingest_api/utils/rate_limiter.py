import time

class RateLimiter:
    """Simple rate limiter for API requests"""
    
    def __init__(self, requests_per_second: int = 40):
        self.requests_per_second = requests_per_second
        self.request_count = 0
        self.start_time = time.time()
    
    def wait_if_needed(self):
        """Wait if approaching rate limit"""
        self.request_count += 1
        
        if self.request_count >= self.requests_per_second:
            elapsed = time.time() - self.start_time
            if elapsed < 1:
                time.sleep(1 - elapsed)
            # Reset counter
            self.request_count = 0
            self.start_time = time.time()

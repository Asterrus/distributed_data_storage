import logging
import random
from collections.abc import Generator
from datetime import datetime, timedelta, timezone

import numpy as np

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

URLS = [
    "/api/login",
    "/api/orders",
    "/api/orders/{id}",
    "/api/products",
    "/api/cart",
]


def generate_random_url():
    url = random.choice(URLS)
    if "{id}" in url:
        return url.replace("{id}", str(random.randint(1, 10000)))
    return url


def generate_status_code():
    r = random.random()
    if r < 0.90:
        return 200
    elif r < 0.97:
        return random.choices(
            [400, 401, 403, 404, 429],
            weights=[1, 1, 1, 5, 1],
        )[0]
    else:
        return random.choices(
            [500, 502, 503, 504],
            weights=[1, 1, 3, 1],
        )[0]


def generate_response_time():
    if random.random() < 0.95:
        return max(1, int(np.random.normal(loc=150, scale=50)))
    return min(2000, int(np.random.exponential(scale=400) + 300))


def random_timestamp(start: datetime, end: datetime) -> datetime:
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    ts = start + timedelta(seconds=random_seconds)
    return ts.replace(tzinfo=timezone.utc)


def generate_log_row(start: datetime, end: datetime):
    row = {
        "timestamp": random_timestamp(start, end),
        "user_id": random.randint(1, 1000),
        "url": generate_random_url(),
        "response_time": generate_response_time(),
        "status_code": generate_status_code(),
    }
    return row


def generate_logs(n) -> Generator[dict, None, None]:
    current_datetime = datetime.now()
    start_datetime = current_datetime - timedelta(days=7)

    for _ in range(n):
        yield generate_log_row(start_datetime, current_datetime)
    logger.info(f"Generated {n} logs")

import sentry_sdk
from sentry_sdk.integrations.redis import RedisIntegration
from sentry_sdk import capture_exception


def init_sentry():
    sentry_sdk.init(
        "https://9d1b5dc9a2124c489a20037c24bd280f@o621052.ingest.sentry.io/5751783",
        integrations=[RedisIntegration()],
        # Set traces_sample_rate to 1.0 to capture 100%
        # of transactions for performance monitoring.
        # We recommend adjusting this value in production.
        traces_sample_rate=1.0
    )

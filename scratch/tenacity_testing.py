from tenacity import (
    AsyncRetrying,
    RetryError,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

import asyncio



async def do_function():
    try:
        async for attempt in AsyncRetrying(
            retry=retry_if_exception_type(Exception),
            stop=stop_after_attempt(10),
            wait=wait_exponential(multiplier=1, min=4, max=60),
        ):
            with attempt:
                print(attempt.retry_state.attempt_number)
                print(attempt.retry_state.idle_for)
                raise Exception("Bad")

    except RetryError as e:
        print(f"Retry Error: {str(e)}")
    finally:
        print("Finally")

if __name__ == "__main__":
    asyncio.run(do_function())
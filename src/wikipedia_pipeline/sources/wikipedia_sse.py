import json
import logging
import random
import time
from typing import Any, Dict, Iterator, Optional

import dlt
import requests

# Configure basic logging for the script if not already configured by DLT
# DLT will typically set up its own logging, but this ensures logs are visible during standalone execution.
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# --- Configuration Constants ---
DEFAULT_API_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
DEFAULT_REQUEST_TIMEOUT_SECONDS = (
    300  # Timeout for initial connection and between bytes
)
DEFAULT_MAX_RETRIES = 5
DEFAULT_BACKOFF_FACTOR_SECONDS = 5
DEFAULT_MAX_BACKOFF_SECONDS = 120


@dlt.source(name="wikipedia_sse")
def wikipedia_sse_stream_source(
    api_url: str = dlt.config.value,
    request_timeout: int = dlt.config.value,
    max_retries: int = dlt.config.value,
    backoff_factor: float = dlt.config.value,
    max_backoff: float = dlt.config.value,
) -> Any:  # Using Any because dlt.source can return various types (generator, list of resources, etc.)
    """
    A DLT source that streams Server-Sent Events (SSE) from the Wikipedia recent changes API.

    This source is designed to be resilient, with configurable retries and backoff
    for network interruptions.

    Args:
        api_url (str): The URL of the SSE stream.
                        Defaults to dlt.config.value, then DEFAULT_API_URL.
        request_timeout (int): Timeout in seconds for the HTTP request (connection and read).
                               Defaults to dlt.config.value, then DEFAULT_REQUEST_TIMEOUT_SECONDS.
        max_retries (int): Maximum number of retries for connection attempts.
                           Defaults to dlt.config.value, then DEFAULT_MAX_RETRIES.
        backoff_factor (float): Base factor for exponential backoff calculation.
                                Defaults to dlt.config.value, then DEFAULT_BACKOFF_FACTOR_SECONDS.
        max_backoff (float): Maximum backoff time in seconds.
                             Defaults to dlt.config.value, then DEFAULT_MAX_BACKOFF_SECONDS.
    """

    # Resolve config values with defaults
    resolved_api_url = api_url if api_url is not None else DEFAULT_API_URL
    resolved_request_timeout = (
        request_timeout
        if request_timeout is not None
        else DEFAULT_REQUEST_TIMEOUT_SECONDS
    )
    resolved_max_retries = (
        max_retries if max_retries is not None else DEFAULT_MAX_RETRIES
    )
    resolved_backoff_factor = (
        backoff_factor if backoff_factor is not None else DEFAULT_BACKOFF_FACTOR_SECONDS
    )
    resolved_max_backoff = (
        max_backoff if max_backoff is not None else DEFAULT_MAX_BACKOFF_SECONDS
    )

    @dlt.resource(name="recent_changes", write_disposition="append")
    def get_recent_changes() -> Iterator[Dict[str, Any]]:
        """
        A DLT resource that yields recent changes events from the Wikipedia SSE stream.
        It handles connection retries with exponential backoff and jitter.
        """
        retries = 0
        session = (
            requests.Session()
        )  # Use a session for potential future header management or connection pooling

        while retries <= resolved_max_retries:
            try:
                logger.info(
                    f"Attempting to connect to SSE stream: {resolved_api_url} (Attempt {retries + 1}/{resolved_max_retries + 1})"
                )
                with session.get(
                    resolved_api_url, stream=True, timeout=resolved_request_timeout
                ) as response:
                    response.raise_for_status()  # Raises HTTPError for bad responses (4xx or 5xx)
                    logger.info(
                        f"Successfully connected to SSE stream: {resolved_api_url}"
                    )
                    retries = 0  # Reset retries on successful connection

                    for line in response.iter_lines():
                        if not line:  # Skip empty lines (often used as keep-alives)
                            continue

                        decoded_line = line.decode("utf-8")
                        if decoded_line.startswith("data:"):
                            # Remove "data:" prefix and strip any leading/trailing whitespace
                            json_data_str = decoded_line[5:].strip()
                            if not json_data_str:  # Handle cases like "data: "
                                logger.debug("Received empty data line, skipping.")
                                continue
                            try:
                                event = json.loads(json_data_str)
                                yield event
                            except json.JSONDecodeError as e:
                                logger.warning(
                                    f"Skipping malformed JSON: {json_data_str}. Error: {e}"
                                )
                                # For production, consider sending to a dead-letter queue or specific error log
                        elif decoded_line.startswith(":"):  # SSE comment line
                            logger.debug(f"Received SSE comment: {decoded_line}")
                        elif decoded_line:  # Other non-empty, non-data lines
                            logger.debug(f"Received non-data SSE line: {decoded_line}")

                    # If the stream ends gracefully (server closes connection without error)
                    logger.info("SSE stream ended gracefully by the server.")
                    return  # Exit the generator

            except requests.exceptions.HTTPError as e:
                # For client-side errors (4xx), usually don't retry indefinitely
                # except for 429 (Too Many Requests)
                if (
                    e.response is not None
                    and 400 <= e.response.status_code < 500
                    and e.response.status_code != 429
                ):
                    logger.error(
                        f"Client error connecting to stream: {e}. Not retrying."
                    )
                    raise  # Re-raise the exception to fail the pipeline
                # For 5xx server errors or 429, retry
                logger.warning(f"HTTP error connecting to stream: {e}. Retrying...")
                # Fall through to retry logic

            except requests.exceptions.RequestException as e:
                # Includes ConnectionError, Timeout, etc.
                logger.warning(f"Stream connection error: {e}. Retrying...")
                # Fall through to retry logic

            except Exception as e:  # Catch any other unexpected errors during streaming
                logger.error(
                    f"Unexpected error during SSE streaming: {e}", exc_info=True
                )
                # Depending on policy, you might want to retry or raise
                # For now, let's treat it like a connection error and retry
                # Fall through to retry logic

            # If we are here, it means an error occurred and we need to retry (or max_retries exceeded)
            retries += 1
            if retries > resolved_max_retries:
                logger.error(
                    f"Max retries ({resolved_max_retries}) exceeded for SSE stream. Giving up."
                )
                # Raise the last known exception or a generic one
                # To make this cleaner, you might store the last exception in the loop
                raise RuntimeError(
                    f"Failed to connect to SSE stream {resolved_api_url} after {resolved_max_retries} retries."
                )

            # Exponential backoff with jitter
            backoff_time = resolved_backoff_factor * (2 ** (retries - 1))
            backoff_time = min(backoff_time, resolved_max_backoff)  # Cap backoff time
            jitter = random.uniform(0, backoff_time * 0.1)  # Add up to 10% jitter
            sleep_time = backoff_time + jitter

            logger.info(f"Waiting {sleep_time:.2f} seconds before next retry...")
            time.sleep(sleep_time)

        # This part should ideally not be reached if the loop logic is correct,
        # as it either returns on success, raises on unrecoverable error, or raises on max_retries.
        # However, as a fallback to make the generator finite if something unexpected happens:
        logger.error("Exited SSE streaming loop unexpectedly.")
        return

    return get_recent_changes


if __name__ == "__main__":
    # --- Example Usage ---

    # To configure, you can create a .dlt/config.toml file in your project root:
    # [sources.wikipedia_sse]
    # api_url = "https://stream.wikimedia.org/v2/stream/recentchange"
    # request_timeout = 300
    # max_retries = 10
    # backoff_factor = 10
    # max_backoff = 300

    # Or set environment variables:
    # export SOURCES__WIKIPEDIA_SSE__API_URL="https://stream.wikimedia.org/v2/stream/recentchange"
    # export SOURCES__WIKIPEDIA_SSE__REQUEST_TIMEOUT=300
    # ... and so on for other parameters

    # Initialize the DLT pipeline
    pipeline = dlt.pipeline(
        pipeline_name="wikipedia_pipeline",
        destination="duckdb",  # or "postgres", "bigquery", etc.
        dataset_name="raw_data_development",  # or any other dataset name
    )

    # Create the source instance. DLT will inject configured values.
    # You can also override them here:
    # stream_source = wikipedia_sse_stream_source(max_retries=3)
    stream_source = wikipedia_sse_stream_source()

    logger.info("Starting Wikipedia SSE stream pipeline run...")
    try:
        load_info = pipeline.run(stream_source)
        logger.info("Pipeline run completed successfully.")
        logger.info(f"Load info:\n{load_info.last_trace}")
    except KeyboardInterrupt:
        logger.info("Pipeline run interrupted by user.")
    except Exception as e:
        logger.error(f"Pipeline run failed: {e}", exc_info=True)

    # Note: For a continuous stream like this, the `pipeline.run()` will
    # run indefinitely until the stream stops, an error occurs that exhausts retries,
    # or it's manually interrupted (e.g., Ctrl+C).
    # In a production orchestrator (like Airflow, Prefect, Dagster), you'd
    # typically run this as a long-running service or a job that processes
    # for a certain duration/number of events if the stream is truly infinite
    # and you want to batch it. DLT itself will micro-batch data as it arrives.

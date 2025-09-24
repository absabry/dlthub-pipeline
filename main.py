# /// script
# dependencies = [
#   "dlt[duckdb,parquet,s3]>=1.8.1",
#   "enlighten>=1.14.1",
# ]
# ///

import os
import time
from collections.abc import Iterator
from typing import Any

import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

BASE_URL = "https://jaffle-shop.scalevector.ai/api/v1"
os.environ["DATA_WRITER__BUFFER_MAX_ITEMS"] = "10000"
os.environ["EXTRACT__WORKERS"] = "60"

jaffle_shop_client = RESTClient(
    base_url=BASE_URL,
    paginator=PageNumberPaginator(
        base_page=1,
        page_param="page",
        total_path=None,
        maximum_page=100,  ## <-- add it mainly for orders
    ),
)


@dlt.resource(name="customers", parallelized=True)
def customers() -> Iterator[list[dict[str, Any]]]:
    for page in jaffle_shop_client.paginate("/customers", params={"page_size": 500}):
        for item in page.response.json():
            item["metadata"] = {"url": page.request.url, "params": dict(page.request.params)}
            yield item


@dlt.resource(name="orders", parallelized=True)
def orders() -> Iterator[list[dict[str, Any]]]:
    for page in jaffle_shop_client.paginate("/orders", params={"page_size": 100}):
        for item in page.response.json():  # noqa: UP028
            yield item


@dlt.resource(name="products", parallelized=True)
def products() -> Iterator[list[dict[str, Any]]]:
    for page in jaffle_shop_client.paginate("/products", params={"page_size": 500}):
        for item in page.response.json():  # noqa: UP028
            yield item

@dlt.source
def jaffle_source():
    return customers(), orders(), products()

if __name__ == "__main__":
    print(os.environ.get("EXTRACT__WORKERS"))
    print(os.environ.get("DATA_WRITER__BUFFER_MAX_ITEMS"))

    pipeline = dlt.pipeline(
        pipeline_name="jaffle_opt",
        destination="duckdb",
        dataset_name="jaffle_db",
        progress="log",
    )

    start = time.time()
    extract_pipeline = pipeline.run(jaffle_source())
    print(pipeline.last_trace)
    print(f"Pipeline finished in {time.time() - start:.2f}s")

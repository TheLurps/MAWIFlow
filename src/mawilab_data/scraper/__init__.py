from importlib.util import find_spec

if not find_spec("scrapy"):
    raise ImportError(
        "Install the scraper extra: `pip install mawilab-data[scraper]`"
    )

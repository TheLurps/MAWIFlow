from scrapy.cmdline import execute
import sys
import os

os.environ.setdefault("SCRAPY_SETTINGS_MODULE", "mawilab_data.scraper.settings")


def main():
    # Support running as `python -m mawilab_data.scraper [spider_name]`
    spider = sys.argv[1] if len(sys.argv) > 1 else "mawilabv1.1"

    print(f"Running Scrapy spider: {spider}")
    execute(["scrapy", "crawl", spider])


if __name__ == "__main__":
    main()

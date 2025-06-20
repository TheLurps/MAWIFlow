# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import yaml
from pathlib import Path


class ScraperPipeline:
    def process_item(self, item, spider):
        return item


class YamlExportPipeline:
    def __init__(self):
        self.items = []

    def process_item(self, item, spider):
        # Store a copy of the item
        self.items.append(dict(item))
        # Log that we've processed an item
        spider.logger.info(f"Processed item: {item}")
        return item

    def close_spider(self, spider):
        # Create filename using spider name
        filename = f"{spider.name}_spider.yaml"
        # Get the current working directory and create the full path
        output_file = Path(Path.cwd(), "data", "raw", filename)

        # Write items to YAML file
        with open(output_file, "w", encoding="utf-8") as f:
            yaml.dump(
                self.items, f, default_flow_style=False, allow_unicode=True
            )

        # Log success message with item count and file location
        spider.logger.info(
            f"Successfully wrote {len(self.items)} items to {output_file}"
        )

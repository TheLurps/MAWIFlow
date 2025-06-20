from scrapy import Spider
import re


class MawilabSpider_v1_1(Spider):
    name = "mawilabv1.1"
    allowed_domains = ["www.fukuda-lab.org", "mawi.wide.ad.jp"]
    start_urls = ["http://www.fukuda-lab.org/mawilab/v1.1"]

    def parse(self, response):
        index_match = re.search(
            r"mawilab/(v\d+\.\d+/\d{4}/\d{2}/\d{2})", response.url
        )
        index = index_match.group(1) if index_match else None

        for link in response.css("a::attr(href)").getall():
            file_types = {
                ".pcap.gz": "tcpdump",
                ".dump.gz": "tcpdump",
                "_anomalous_suspicious.xml": "anomalous-suspicious-admd",
                "_anomalous_suspicious.csv": "anomalous-suspicious-csv",
                "_notice.xml": "notice-admd",
                "_notice.csv": "notice-csv",
            }

            if index_match:
                for suffix, file_type in file_types.items():
                    if link.endswith(suffix):
                        yield {
                            "index": index,
                            "type": file_type,
                            "url": response.urljoin(link),
                            "filename": link.split("/")[-1],
                        }
                        break

            next_page = response.urljoin(link)
            if next_page.endswith(".html"):
                yield response.follow(next_page, callback=self.parse)

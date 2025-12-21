# demo.py
from scrapy.crawler import CrawlerProcess
from ecommerce_spider.spiders.shopify_crawl import ShopifyCrawlFastSpider


def run_batch(sites: list[dict]):
    """
    sites = [
        {"domain": "...", "category": "..."},
        {"domain": "...", "category": "..."},
    ]
    """

    process = CrawlerProcess(settings={
        # ==== 性能（极速版推荐）====
        "CONCURRENT_REQUESTS": 256,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 32,
        "DOWNLOAD_DELAY": 0,
        "AUTOTHROTTLE_ENABLED": False,
        "RETRY_TIMES": 3,
        "LOG_LEVEL": "INFO",
        "ROBOTSTXT_OBEY": False,
        "FEEDS": {},

        # ==== 导出 ====
        "ITEM_PIPELINES": {
            "ecommerce_spider.pipelines.PandasExporter": 300,
        },
        "PANDAS_FIELDS": [
            "SKU", "Name", "Categories", "Regular price", "cf_opingts",
            "Description", "Images", "自定义分类", "原站域名", "分布网站识别", "语言"
        ],
    })

    for site in sites:
        domain = site["domain"]
        category = site.get("category", "未知分类")

        site_name = domain.split("//")[-1].replace(".", "_")
        export_file = f"{site_name}.xlsx"

        process.crawl(
            ShopifyCrawlFastSpider,
            domain=domain,
            category=category,
            export_file=export_file,  # 👈 关键
        )

    process.start()
    print("\n全部站点爬取完成\n")


if __name__ == "__main__":
    sites = [
        # {"domain": "https://shibuya-stationery.com", "category": "办公用品"},
        # {"domain": "https://ewartwoods.com", "category": "办公用品"},
        {"domain": "https://www.lagirlusa.com", "category": "艺术与娱乐"},
        # {"domain": "https://www.bando.com", "category": "办公用品"},
        # {"domain": "https://tasklinesupplies.com", "category": "办公用品"},
    ]

    run_batch(sites)

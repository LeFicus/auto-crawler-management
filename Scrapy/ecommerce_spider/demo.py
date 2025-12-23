# demo.py
import os

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
            "SKU", "Name", "Description", "Regular price", "Categories",
            "Images", "cf_opingts","自定义分类", "原站域名", "分布网站识别", "语言"
        ],
    })

    for site in sites:
        domain = site["domain"]
        category = site.get("category", "未知分类")

        site_name = domain.split("//")[-1].replace(".", "_").replace("/", "")

        # ✅ category 作为目录名（可自行再清洗）
        category_dir = category.strip()
        category_dir = category_dir.replace("/", "_")
        # ✅ 创建目录（已存在不会报错）
        os.makedirs(category_dir, exist_ok=True)
        export_file = os.path.join(category_dir, f"{site_name}.xlsx")
        process.crawl(
            ShopifyCrawlFastSpider,
            domain=domain,
            category=category,
            export_file=export_file,  # 👈 关键
        )

    process.start()

if __name__ == "__main__":
    sites = [
        # {"domain":"https://www.corston.eu", "category": "五金/硬件"},
        # {"domain":"https://nyhardware.com", "category": "五金/硬件"},

        # {"domain":"https://www.levenger.com", "category": "办公用品"},
        # {"domain":"https://riflepaperco.com", "category": "办公用品"},
        # {"domain":"https://shophorne.com", "category": "家具"},

        # {"domain":"https://www.mcgeeandco.com", "category": "家居与园艺"},
        # {"domain":"https://www.bludot.com", "category": "家居与园艺"},
        # {"domain":"https://redhead-drinking-creations.myshopify.com", "category": "厨房/餐厅"},
        # {"domain":"https://market99.com", "category": "厨房/餐厅"},
        # {"domain":"https://superdokan.com", "category": "厨房/餐厅"},
        # {"domain":"https://myborosil.com/", "category": "厨房/餐厅"},
        {"domain":"https://koreanskincare.nl", "category": "美妆"},
    ]

    run_batch(sites)

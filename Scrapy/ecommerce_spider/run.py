from scrapy.crawler import CrawlerProcess
from ecommerce_spider.spiders.woo_crawl import WooCrawlSpider


def run(domain: str, category: str = "未知分类", config_file: str = None):
    if not domain or not domain.startswith("http"):
        print("请传入正确的域名，例如：https://bazaarica.com/sitemaps/en-us/sitemap.xml")
        return

    # 动态生成文件名
    site_name = domain.split("//")[-1].replace(".", "_")
    export_file = f"{site_name}.xlsx"

    process = CrawlerProcess(settings={
        "PANDAS_EXPORT_FILE": export_file,
        "PANDAS_FIELDS": [
            "SKU", "Name", "Categories", "Regular price", "cf_opingts",
            "Description", "Images", "自定义分类", "原站域名", "分布网站识别", "语言"
        ],
        # ==== 核心提速设置 ====
        "CONCURRENT_REQUESTS": 16,  # 全局并发请求数，建议 16-64
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,  # 每个域名最大并发（防单个站点被封）
        "CONCURRENT_REQUESTS_PER_IP": 0,  # 通常保持 0，除非你用代理

        "DOWNLOAD_DELAY": 0.5,  # 基础延迟降到 0.5 秒
        "RANDOMIZE_DOWNLOAD_DELAY": True,  # 随机化延迟（0.25-0.75秒），防封

        # ==== 启用 AutoThrottle 自动调节（最推荐！）====
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 8.0,  # 目标：每个域名平均 8 个并发
        "AUTOTHROTTLE_START_DELAY": 0.5,  # 起始延迟
        "AUTOTHROTTLE_MAX_DELAY": 10.0,  # 最大延迟（如果站点慢会自动增加）

        # ==== 其他性能优化 ====
        "RETRY_TIMES": 3,  # 减少重试次数（避免卡住）
        "DOWNLOAD_TIMEOUT": 15,  # 超时 15 秒，快速丢弃慢请求
        "REDIRECT_ENABLED": False,  # 禁用重定向，节省时间
        "COOKIES_ENABLED": False,  # Woo 站一般不需要 cookie
        "LOG_LEVEL": "INFO",  # 减少日志输出
        "HTTPCACHE_ENABLED": True,  # 启用缓存，重复跑时超快（开发测试用）

        # ==== 其他原有设置保持不变 ====
        "ITEM_PIPELINES": {'ecommerce_spider.pipelines.PandasExporter': 300},
        "DOWNLOADER_MIDDLEWARES": {
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'ecommerce_spider.middlewares.CustomUserAgentMiddleware': 400,
        },
    })
    process.crawl(WooCrawlSpider, domain=domain, category=category, config_file=config_file)
    process.start()          # 阻塞直到爬完
    print(f"\n完成！文件已保存：{export_file}\n")


if __name__ == "__main__":
    # 这里改成你想爬的站

    run("https://koreanskincare.nl/sitemap.xml", "艺术与娱乐",config_file="configs/selectors/test.json")
    # run("https://sachdevabeauty.com/sitemaps.xml", "艺术与娱乐", config_file="configs/selectors/sachdevabeauty_com.json")
    # run("https://sachdevabeauty.com/sitemaps.xml", "艺术与娱乐", config_file="configs/selectors/sachdevabeauty_com.json")
    # run("https://www.allbeauty.om/sitemapindex-product.xml.gz", "艺术与娱乐")
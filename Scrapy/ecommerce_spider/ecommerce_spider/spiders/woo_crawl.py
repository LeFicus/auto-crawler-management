import json
import os
import scrapy
from datetime import datetime
from urllib.parse import urlparse


class WooCrawlSpider(scrapy.Spider):
    name = "woo_crawl"



    def __init__(self, domain=None, category="未知分类", *args, **kwargs):
        super().__init__(*args, **kwargs)

        # 动态传入的域名和分类
        if not domain or not domain.startswith("http"):
            raise ValueError("必须传入正确的 domain 参数，例如：https://us.kipling.com/sitemap_index.xml")
        self.domain = domain.rstrip("/")
        self.custom_category = category.strip() or "未知分类"

        # 动态生成导出文件名
        parsed_url = urlparse(self.domain)
        site_name = parsed_url.netloc.replace(".", "_")
        self.export_file = f"{site_name}.xlsx"

        # 修复：正确配置允许的域名（取站点地图域名）
        self.allowed_domains = [parsed_url.netloc]
        self.logger.info(f"允许的域名：{self.allowed_domains}")

        self.seen_handles = set()  # 防重
        self.seen_product_urls = set()  # 商品URL去重

        # 新增：加载汇率文件
        self.exchange_rates = {"USD": 1.0}  # 默认至少有 USD
        self.shop_currency = "USD"  # 默认店铺货币
        rates_path = "exchange_rates.json"

        if os.path.exists(rates_path):
            try:
                with open(rates_path, 'r', encoding='utf-8') as f:
                    loaded_rates = json.load(f)
                    self.exchange_rates.update(loaded_rates)
                self.logger.info(f"成功加载汇率文件：{rates_path}，共 {len(self.exchange_rates)} 种货币")
            except Exception as e:
                self.logger.error(f"加载汇率文件失败：{e}，使用默认 USD=1.0")
        else:
            self.logger.warning(f"未找到汇率文件 {rates_path}，价格将不进行转换（假设 USD）")

    # 修复：使用Scrapy 2.13+推荐的start()方法（替代start_requests）
    async def start(self):
        yield scrapy.Request(
            url=self.domain,
            callback=self.parse_meta_currency,
            priority=100,
            dont_filter=True  # 强制请求，避免被过滤
        )

    def parse_meta_currency(self, response):
        if response.status == 200:
            try:
                # 解析站点地图索引，提取商品站点地图链接
                sitemap_links = response.xpath(
                    '//*[local-name()="sitemap"]/*[local-name()="loc"][contains(text(), "product-")]/text()'
                ).extract()

                if sitemap_links:
                    self.logger.info(f"找到 {len(sitemap_links)} 个商品站点地图链接")
                    for link in sitemap_links:
                        yield scrapy.Request(
                            url=link.strip(),
                            callback=self.parse_product_sitemap,
                            dont_filter=True  # 强制请求
                        )
                else:
                    self.logger.warning("未找到商品站点地图链接，尝试直接解析当前页面")
                    # 直接从当前页面提取商品URL
                    yield from self.parse_product_sitemap(response)

            except Exception as e:
                self.logger.error(f"解析站点地图索引失败：{e}")
        else:
            self.logger.error(f"站点地图访问失败，状态码：{response.status}")

    def parse_product_sitemap(self, response):
        """解析商品站点地图，提取商品详情页URL并发起请求"""
        # 提取所有<loc>标签中的商品URL
        product_urls = response.xpath(
            '//*[local-name()="url"]/*[local-name()="loc"]/text()'
        ).extract()

        # 去重并发起详情页请求
        valid_count = 0
        for url in product_urls:
            url = url.strip()
            if url and url not in self.seen_product_urls:
                self.seen_product_urls.add(url)
                # valid_count += 1
                # # 发起商品详情页请求
                # yield scrapy.Request(
                #     url=url,
                #     callback=self.parse_product_detail,
                #     meta={"category": self.custom_category},
                #     dont_filter=True
                # )

        self.logger.info(f"从站点地图提取到 {valid_count} 个唯一商品URL（总计：{len(self.seen_product_urls)}）")

    def parse_product_detail(self, response):
        """解析商品详情页，提取核心信息并生成Item"""
        if response.status != 200:
            self.logger.warning(f"详情页 {response.url} 返回 {response.status}")
            return

        try:
            # 提取商品核心信息（适配WooCommerce详情页结构）
            item = {
                "SKU": response.xpath('//span[@class="sku"]/text()').extract_first() or "",
                "Name": response.xpath('//h1[@class="product_title entry-title"]/text()').extract_first() or "",
                "Categories": response.meta.get("category", "未知分类"),
                "Regular price": response.xpath(
                    '//p[@class="price"]/span[@class="woocommerce-Price-amount amount"]/text()').re_first(
                    r'(\d+\.?\d*)') or "0.00",
                "cf_opingts": "",
                "Description": " ".join(response.xpath(
                    '//div[@class="woocommerce-product-details__short-description"]//text()').extract()).strip() or "",
                "Images": response.xpath(
                    '//div[@class="woocommerce-product-gallery__image"]/a/@href').extract_first() or "",
                "自定义分类": self.custom_category,
                "原站域名": urlparse(response.url).netloc,
                "分布网站识别": 0,
                "语言": "en",
            }

            # 价格转换（USD）
            try:
                price = float(item["Regular price"])
                rate = self.exchange_rates.get(self.shop_currency, 1.0)
                item["Regular price"] = f"{price * rate:.2f}"
            except:
                item["Regular price"] = "0.00"

            yield item
            self.logger.info(f"成功解析商品：{item['Name'][:50]}... (SKU: {item['SKU']})")

        except Exception as e:
            self.logger.error(f"解析商品详情失败 {response.url}：{repr(e)}")
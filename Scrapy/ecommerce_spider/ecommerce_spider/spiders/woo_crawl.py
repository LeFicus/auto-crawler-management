import hashlib
import json
import os
import scrapy
from datetime import datetime
from urllib.parse import urlparse
import re
from bs4 import BeautifulSoup
class WooCrawlSpider(scrapy.Spider):
    name = "woo_crawl"

    CATEGORY_SKU_MAP = {
        "五金/硬件": "HARD",
        "交通工具/汽车/飞机/船舶": "VEH",
        "体育用品": "SPORT",
        "保健/美容/卫生/护理": "CARE",
        "办公用品": "OFFC",
        "动物/宠物用品": "PET",
        "商业/工业": "IND",
        "婴幼儿用品": "BABY",
        "媒体": "MEDIA",
        "宗教/仪式": "RITE",
        "家具": "FURN",
        "家居与园艺": "HOME",
        "成人": "ADULT",
        "服饰与配饰": "APP",
        "玩具/游戏": "TOY",
        "电子产品": "ELEC",
        "相机与光学器件": "OPT",
        "箱包": "BAG",
        "艺术与娱乐": "ART",
        "软件": "SOFT",
        "饮食/烟酒": "FOOD",
    }
    def __init__(self, domain=None, category="未知分类", config_file=None, *args, **kwargs):
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
        self.exchange_rates = {}  # 默认至少有 USD
        base_dir = os.path.dirname(os.path.abspath(__file__))

        rate_path = os.path.join(base_dir, "exchange_rates.json")

        if os.path.exists(rate_path):
            with open(rate_path, "r", encoding="utf-8") as f:
                self.exchange_rates.update(json.load(f))
                self.logger.info(f"已加载汇率文件: {rate_path}")
        else:
            self.logger.warning(f"未找到汇率文件: {rate_path}")

        # 加载 selectors 配置
        self.selectors = {
            "title": (
                "//h1[@class='product_title entry-title']/text() | "
                "//h1[contains(@class, 'product-title')]/text() | "
                "//h1[contains(@class, 'product_title')]/text() | "
                "//h1[@class='product-title']/text() | "
                "//header//h1/text() | "
                "//div[contains(@class, 'summary')]//h1/text()"
            ),
            "sku": (
                "//span[@class='sku']/text() | "
                "//span[@class='sku_wrapper']//span[@class='sku']/text() | "
                "//div[contains(@class, 'product-meta')]//span[@class='sku']/text() | "
                "//meta[@itemprop='sku']/@content | "
                "//dd[contains(@class, 'variation-SKU')]//text()"
            ),
            "price": (
                "//p[@class='price']//span[@class='woocommerce-Price-amount']/bdi/text() | "
                "//p[@class='price']//span[@class='woocommerce-Price-amount amount']/text() | "
                "//ins//span[@class='woocommerce-Price-amount amount']/bdi/text() | "
                "//span[@class='woocommerce-Price-amount amount']/bdi/text() | "
                "//div[contains(@class, 'summary')]//p[@class='price']//bdi/text() | "
                "//meta[@itemprop='price']/@content"
            ),
            "price_regex": r'[\d.,]+',
            "description": (
                "//div[@class='woocommerce-product-details__short-description']//text() | "
                "//div[contains(@class, 'woocommerce-tabs')]//div[@id='tab-description']//p//text() | "
                "//div[contains(@class, 'woocommerce-tabs')]//div[@id='tab-description']//text() | "
                "//div[contains(@class, 'product-short-description')]//text() | "
                "//div[@itemprop='description']//text()"
            ),
            "images": (
                "//div[@class='woocommerce-product-gallery__image']/a/@href | "
                "//div[@class='woocommerce-product-gallery__image']//img/@src | "
                "//figure[contains(@class, 'woocommerce-product-gallery__wrapper')]//img/@data-large_image | "
                "//figure[contains(@class, 'woocommerce-product-gallery__wrapper')]//a/@href | "
                "//meta[@property='og:image']/@content | "
                "//div[contains(@class, 'product-images')]//img/@src"
            ),
            "currency": "USD",

            # ====== 新增：面包屑分类多备选 XPath ======
            "breadcrumb_links": (
                "//nav[contains(@class, 'woocommerce-breadcrumb')]//a//text() | "
                "//div[contains(@class, 'breadcrumbs')]//a//text() | "
                "//ul[contains(@class, 'breadcrumb')]//a//text() | "
                "//div[contains(@class, 'breadcrumb')]//a//text() | "
                "//div[contains(@class, 'woo-breadcrumbs')]//a//text() | "
                "//nav[@class='breadcrumbs']//a//text() | "
                "//div[@class='product_meta']//a[contains(@href, '/product-category/')]//text()"
            ),
            # 可选：也提取最后一个（商品名），用于过滤
            "breadcrumb_last": (
                "//nav[contains(@class, 'woocommerce-breadcrumb')]//span[contains(@class, 'breadcrumb-last')]//text() | "
                "//nav[contains(@class, 'woocommerce-breadcrumb')]//a[last()]//text()"
            )
        }

        if config_file and os.path.exists(config_file):
            try:
                with open(config_file, 'r', encoding='utf-8') as f:
                    custom_selectors = json.load(f)
                self.selectors.update(custom_selectors)
                self.logger.info(f"加载自定义 selectors 配置：{config_file}")
            except Exception as e:
                self.logger.error(f"加载配置失败：{e}")
        else:
            self.logger.warning(f"当前工作目录: {os.getcwd()}")
            self.logger.warning(f"蜘蛛文件目录: {os.path.dirname(os.path.abspath(__file__))}")
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
                sitemap_links = response.xpath(self.selectors["site_map"]).extract()

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
                valid_count += 1
                # 发起商品详情页请求
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_product_detail,
                    dont_filter=True
                )

        self.logger.info(f"从站点地图提取到 {valid_count} 个唯一商品URL（总计：{len(self.seen_product_urls)}）")

    def category_prefix(self) -> str:
        return self.CATEGORY_SKU_MAP.get(self.custom_category, "GEN")

    def product_code(self, value, length: int = 6) -> str:
        return hashlib.md5(str(value or "").encode("utf-8")).hexdigest()[:length].upper()

    def parse_product_detail(self, response):
        """解析商品详情页，提取核心信息并生成Item"""
        if response.status != 200:
            self.logger.warning(f"详情页 {response.url} 返回 {response.status}")
            return

        try:
            # ====== 基础字段提取 ======
            name = response.xpath(self.selectors["title"]).get(default="").strip()

            # SKU（原始，可能为空）
            original_sku = response.xpath(self.selectors["sku"]).get(default="").strip()

            # Description：必须用 getall() 合并多个文本节点
            description = response.xpath(self.selectors["description"]).get(default="").strip()
            if description:
                soup = BeautifulSoup(description, 'html.parser')
                # 移除图片、视频、iframe等
                for tag in soup.find_all(['img', 'video', 'iframe', 'script', 'style']):
                    tag.decompose()
                # 可选：移除空段落
                for p in soup.find_all(['p', 'div']):
                    if not p.get_text(strip=True):
                        p.decompose()

                description = str(soup)  # 转回字符串，干净的HTML
            else:
                description = ""
            # 主图
            images = response.xpath(self.selectors["images"]).get(default="").strip()

            # ====== 生成唯一 SKU ======
            # 用 URL 的最后一部分（通常是商品 slug）生成唯一 hash
            url_slug = response.url.split("/")[-1].split("?")[0]  # 去掉查询参数
            if not url_slug:
                url_slug = response.url

            sku_hash = self.product_code(url_slug, length=8)  # 8位足够唯一

            sku_prefix = self.category_prefix()
            sku = f"{sku_prefix}-{sku_hash}"

            # 如果有原始 SKU，附加在后面（可选，提高可读性）
            if original_sku:
                original_hash = self.product_code(original_sku, length=4)
                sku = f"{sku}-{original_hash}"

            # ====== 自动面包屑分类 ======
            breadcrumb_items = response.xpath(self.selectors["breadcrumb_links"]).getall()
            breadcrumb_items = [item.strip() for item in breadcrumb_items if item.strip()]

            # 移除最后一个（通常是商品名）
            last_crumb = response.xpath(self.selectors["breadcrumb_last"]).get()
            if last_crumb:
                last_crumb = last_crumb.strip()
                if last_crumb and breadcrumb_items and breadcrumb_items[-1] == last_crumb:
                    breadcrumb_items = breadcrumb_items[:-1]

            # 过滤 Home
            filtered_categories = [cat for cat in breadcrumb_items if cat.lower() != "home"]

            # 取前两个有效分类，用 "|||" 分隔
            if len(filtered_categories) >= 2:
                categories_from_breadcrumb = "|||".join(filtered_categories[:2])
            elif len(filtered_categories) == 1:
                categories_from_breadcrumb = filtered_categories[0]
            else:
                categories_from_breadcrumb = "Others"

            final_category = categories_from_breadcrumb

            price_texts = response.xpath(self.selectors["price"]).getall()
            price_texts = [t.strip() for t in price_texts if t.strip()]  # 清理空字符串
            price_values = set()
            for price_text in price_texts:
                price_values.add(price_text)
            price_num = 0.0
            if price_values:
                # 遍历所有提取到的价格文本，取第一个能成功转换为数字的
                for raw_price in price_values:
                    # 使用正则提取数字部分（支持 ₹2,599 或 2,599 或 2599）
                    match = re.search(self.selectors.get("price_regex", r'[\d.,]+'), raw_price)
                    if match:
                        num_str = match.group(0)  # '2,599'
                        num_str = num_str.replace(",", "")  # '2599'
                        try:
                            price_num = float(num_str)
                            if price_num > 0:  # 确保不是0
                                break  # 找到第一个有效价格就停止
                        except ValueError:
                            continue  # 转换失败，试下一个

            # 如果上面没找到，再尝试从 meta itemprop='price' 拿（有些主题会放这里）
            if price_num == 0.0:
                meta_price = response.xpath("//meta[@itemprop='price']/@content").get(default="")
                if meta_price:
                    cleaned = meta_price.replace(",", "")
                    try:
                        price_num = float(cleaned)
                    except ValueError:
                        pass

            currency = self.selectors.get("currency")
            rate = self.exchange_rates.get(currency, 1.0)
            price_clean = f"{price_num * rate:.2f}"
            self.logger.info(f"当前货币:汇率 {currency}:{rate} - 原价格：{price_num} - 汇率转换后的价格{price_clean}")

            # ====== 组装 Item ======
            item = {
                "SKU": sku,
                "Name": name,
                "Description": description,
                "Regular price": price_clean,
                "Categories": final_category,
                "Images": images,
                "cf_opingts": "",
                "自定义分类": self.custom_category,
                "原站域名": urlparse(response.url).netloc,
                "分布网站识别": 0,
                "语言": "en",
            }

            self.logger.info(
                f"成功生成商品 → SKU: {item['SKU']} | "
                f"Name: {item['Name'][:50]}... | "
                f"Price: {item['Regular price']} | "
                f"Categories: {item['Categories']}"
                f"商品URL: {response.url}"
            )

            yield item

        except Exception as e:
            self.logger.error(f"解析商品详情失败 {response.url}: {repr(e)}")
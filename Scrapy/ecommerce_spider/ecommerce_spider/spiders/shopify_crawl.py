import hashlib
import json
import os
import re
from urllib.parse import urljoin, urlparse

import scrapy
from scrapy.exceptions import CloseSpider
from lxml import html  # 新增：用于解析和清理HTML


class ShopifyCrawlFastSpider(scrapy.Spider):
    name = "shopify_crawl_fast"

    # 分类SKU映射表
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

    # 爬虫配置优化
    custom_settings = {
        "CONCURRENT_REQUESTS": 64,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 16,
        "DOWNLOAD_DELAY": 0.2,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 8.0,
        "RETRY_TIMES": 3,
        "LOG_LEVEL": "INFO",
        "ROBOTSTXT_OBEY": False,
        "DOWNLOAD_TIMEOUT": 30,
        "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }

    def __init__(self, domain=None, category="未知分类", export_file=None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if not domain or not domain.startswith(("http://", "https://")):
            raise ValueError("domain 参数必须是完整的 URL（以 http:// 或 https:// 开头）")

        self.domain = domain.rstrip("/")
        self.custom_category = category.strip() or "未知分类"
        self.export_file = export_file

        parsed_url = urlparse(self.domain)
        self.allowed_domains = [parsed_url.netloc.replace("www.", "")]

        self.page = 1
        self.limit = 250

        self.shop_currency = "USD"
        self.exchange_rates = {}
        self.processed_variant_ids = set()

        self._load_exchange_rates()

    def _load_exchange_rates(self):
        """加载汇率文件"""
        try:
            base_dir = os.path.dirname(os.path.abspath(__file__))
            rate_path = os.path.join(base_dir, "exchange_rates.json")

            if os.path.exists(rate_path):
                with open(rate_path, "r", encoding="utf-8") as f:
                    self.exchange_rates = json.load(f)
                    self.logger.info(f"成功加载汇率文件: {rate_path}")
            else:
                self.logger.warning(f"未找到汇率文件: {rate_path}，将使用默认汇率 1.0")
        except Exception as e:
            self.logger.error(f"加载汇率文件出错: {str(e)}，将使用默认汇率 1.0")
            self.exchange_rates = {}

    # ---------- 新增：清理HTML中的a标签 ----------
    def remove_a_tags(self, html_content):
        """
        清理HTML中的<a>标签
        :param html_content: 原始HTML字符串
        :return: 清理后的HTML字符串
        """
        if not html_content or html_content.strip() == "":
            return ""

        try:
            # 解析HTML
            tree = html.fromstring(html_content)

            # 方案1：移除所有<a>标签及其内容（推荐）
            for a_tag in tree.xpath("//a"):
                a_tag.getparent().remove(a_tag)

            # 重新构建HTML字符串
            cleaned_html = html.tostring(tree, encoding='unicode', method='html')

            # 清理多余的空白和换行
            cleaned_html = re.sub(r'\n\s*\n', '\n', cleaned_html).strip()

            return cleaned_html
        except Exception as e:
            self.logger.error(f"清理a标签出错: {str(e)}")
            # 出错时返回原始内容（避免数据丢失）
            return html_content

    # ---------- 工具方法 ----------
    def category_prefix(self) -> str:
        """获取分类前缀"""
        return self.CATEGORY_SKU_MAP.get(self.custom_category, "GEN")

    def product_code(self, value, length: int = 6) -> str:
        """生成产品编码（MD5）"""
        if not value:
            return "".zfill(length).upper()
        return hashlib.md5(str(value).encode("utf-8")).hexdigest()[:length].upper()

    def build_variant_title(self, variant):
        """构建变体标题（清理无效值）"""
        options = []
        for opt in ["option1", "option2", "option3"]:
            val = str(variant.get(opt, "")).strip()
            if val and val not in ["None", "Default Title"]:
                options.append(val)
        return " ".join(options)

    def clean_sku(self, sku):
        """清理SKU中的特殊字符（确保SKU规范）"""
        sku = re.sub(r'[^\w\-]', '', sku.replace(" ", "_"))
        return sku[:50].strip("_-")

    # ---------- 爬虫入口 ----------
    def start_requests(self):
        """开始请求：先获取店铺元信息"""
        meta_url = urljoin(self.domain, "/meta.json")
        yield scrapy.Request(
            meta_url,
            callback=self.parse_meta,
            errback=self.meta_failed,
            priority=100,
            dont_filter=True
        )

    def parse_meta(self, response):
        """解析店铺元信息（获取币种）"""
        try:
            meta_data = json.loads(response.text)
            self.shop_currency = str(meta_data.get("currency", "USD")).upper()
            self.logger.info(
                f"店铺币种: {self.shop_currency}, 汇率: {self.exchange_rates.get(self.shop_currency, 1.0)}")
        except json.JSONDecodeError:
            self.logger.error("解析meta.json失败，使用默认币种USD")
            self.shop_currency = "USD"
        except Exception as e:
            self.logger.error(f"处理meta.json出错: {str(e)}，使用默认币种USD")
            self.shop_currency = "USD"

        yield from self.request_page()

    def meta_failed(self, failure):
        """meta请求失败的处理"""
        self.logger.error(f"获取meta.json失败: {failure.getErrorMessage()}")
        self.shop_currency = "USD"
        yield from self.request_page()

    # ---------- 分页处理 ----------
    def request_page(self):
        """请求指定页码的商品列表"""
        products_url = urljoin(self.domain, f"/products.json?limit={self.limit}&page={self.page}")
        yield scrapy.Request(
            products_url,
            callback=self.parse_products,
            dont_filter=True,
            meta={"page": self.page}
        )

    def parse_products(self, response):
        """解析商品列表"""
        try:
            data = json.loads(response.text)
        except json.JSONDecodeError:
            self.logger.error(f"第{self.page}页JSON解析失败: {response.url}")
            if self.page > 3:
                raise CloseSpider("多次解析商品数据失败，停止爬虫")
            self.page += 1
            yield from self.request_page()
            return

        products = data.get("products", [])
        current_page = response.meta.get("page", 1)

        if not products:
            self.logger.info(f"第{current_page}页无商品数据，商品抓取完成")
            return

        rate = float(self.exchange_rates.get(self.shop_currency, 1.0))

        for product in products:
            try:
                yield from self.parse_single_product(product, rate)
            except Exception as e:
                self.logger.error(f"解析商品失败 (ID: {product.get('id')}): {str(e)}")
                continue

        if len(products) == self.limit:
            self.page += 1
            self.logger.info(f"第{current_page}页处理完成，继续第{self.page}页")
            yield from self.request_page()
        else:
            self.logger.info(f"第{current_page}页为最后一页，抓取结束")

    def parse_single_product(self, product, rate):
        """解析单个商品的所有变体"""
        product_id = product.get("id")
        title = str(product.get("title", "")).strip().title()

        # ---------- 修改：清理description中的a标签 ----------
        raw_desc = str(product.get("body_html", "")).strip()
        desc = self.remove_a_tags(raw_desc)  # 调用清理方法

        product_category = str(product.get("product_type", "Others")).strip().title()
        if product_category == "" or product_category is None:
            product_category = "Others"
        product_images = product.get("images", [])

        image_map = {img.get("id"): img.get("src", "") for img in product_images}

        for variant in product.get("variants", []):
            variant_id = variant.get("id")

            if variant_id in self.processed_variant_ids:
                continue
            self.processed_variant_ids.add(variant_id)

            try:
                variant_title = self.build_variant_title(variant)

                base_sku = f"{self.category_prefix()}-{self.product_code(variant_id)}"
                if variant_title:
                    base_sku += f"-{variant_title.replace(' ', '_')}"
                sku = self.clean_sku(base_sku)

                price_str = str(variant.get("price", 0.0)).strip()
                try:
                    price = float(price_str) * rate if price_str else 0.0
                    usd_price = round(price, 2)
                except ValueError:
                    usd_price = 0.0
                    self.logger.warning(f"变体{variant_id}价格解析失败: {price_str}")

                variant_image_id = variant.get("id")
                variant_image = image_map.get(variant_image_id, "")
                if not variant_image and product_images:
                    variant_image = product_images[0].get("src", "")

                full_name = f"{title} {self.clean_sku(variant_title)}".strip()
                full_name = full_name.replace("Default Title", "").strip().title()

                item = {
                    "SKU": sku,
                    "Name": full_name,
                    "Description": desc,  # 使用清理后的描述
                    "Regular price": usd_price,
                    "Categories": product_category,
                    "Images": variant_image,
                    "cf_opingts": "",
                    "自定义分类": self.custom_category,
                    "原站域名": urlparse(self.domain).netloc,
                    "分布网站识别": 0,
                    "语言": "en",
                }

                yield item

            except Exception as e:
                self.logger.error(f"解析变体{variant_id}失败: {str(e)}")
                continue
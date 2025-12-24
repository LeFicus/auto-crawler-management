import hashlib
import json
import os
import scrapy


class ShopifyCrawlFastSpider(scrapy.Spider):
    name = "shopify_crawl_fast"

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

    custom_settings = {
        "CONCURRENT_REQUESTS": 256,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 32,
        "DOWNLOAD_DELAY": 0,
        "AUTOTHROTTLE_ENABLED": False,
        "RETRY_TIMES": 2,
        "LOG_LEVEL": "INFO",
        "ROBOTSTXT_OBEY": False,
    }

    def __init__(self, domain=None, category="未知分类",export_file=None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if not domain or not domain.startswith("http"):
            raise ValueError("domain 必须是完整 URL")
        self.export_file = export_file

        self.domain = domain.rstrip("/")
        self.custom_category = category.strip() or "未知分类"

        self.allowed_domains = [
            self.domain.replace("https://", "")
            .replace("http://", "")
            .replace("www.", "")
        ]

        self.page = 1
        self.limit = 250

        self.shop_currency = "USD"
        self.exchange_rates = {}
        base_dir = os.path.dirname(os.path.abspath(__file__))

        rate_path = os.path.join(base_dir, "exchange_rates.json")
        self.logger.info(rate_path)
        if os.path.exists(rate_path):
            with open(rate_path, "r", encoding="utf-8") as f:
                self.exchange_rates.update(json.load(f))
                self.logger.info(f"已加载汇率文件: {rate_path}")
        else:
            self.logger.warning(f"未找到汇率文件: {rate_path}")

    # ---------- helpers ----------

    def category_prefix(self) -> str:
        return self.CATEGORY_SKU_MAP.get(self.custom_category, "GEN")

    def product_code(self, value, length: int = 6) -> str:
        return hashlib.md5(str(value or "").encode("utf-8")).hexdigest()[:length].upper()

    def build_variant_title(self, variant):
        return " ".join(
            v for v in [
                str(variant.get("option1", "")).strip(),
                str(variant.get("option2", "")).strip(),
                str(variant.get("option3", "")).strip(),
            ] if v
        )

    # ---------- start ----------

    def start_requests(self):
        yield scrapy.Request(
            f"{self.domain}/meta.json",
            callback=self.parse_meta,
            errback=self.meta_failed,
            priority=100,
        )

    def parse_meta(self, response):
        try:
            self.shop_currency = json.loads(response.text).get("currency", "USD").upper()
            self.logger.info(f"币种={self.shop_currency}, 汇率={self.exchange_rates.get(self.shop_currency)}")

        except Exception:
            self.shop_currency = "USD"
            self.logger.info(f"币种={self.shop_currency}, 汇率={self.exchange_rates.get(self.shop_currency)}")

        yield from self.request_page()

    def meta_failed(self, _):
        self.shop_currency = "USD"
        yield from self.request_page()

    # ---------- pagination (page) ----------

    def request_page(self):
        url = f"{self.domain}/products.json?limit={self.limit}&page={self.page}"
        yield scrapy.Request(url, callback=self.parse_products, dont_filter=True)

    def parse_products(self, response):
        data = json.loads(response.text)
        products = data.get("products", [])

        if not products:
            self.logger.info("商品抓取完成")
            return

        rate = self.exchange_rates.get(self.shop_currency, 1.0)

        for product in products:
            title = product.get("title", "")
            desc = product.get("body_html", "")
            category = product.get("product_type")
            if category is None:
                category = "Others"

            images = product.get("images") or []
            variant_image = images[0].get("src", "") if images else ""

            for variant in product.get("variants", []):
                option_title = self.build_variant_title(variant).replace("None",'')
                sku = (
                    f"{self.category_prefix()}-"
                    f"{self.product_code(variant.get('id'))}"
                )

                try:
                    price = float(variant.get("price") or 0,) * rate
                    usd_price = round(price, 2)
                except Exception:
                    usd_price = 0.0

                yield {
                    "SKU": sku,
                    "Name": f"{title} {option_title}".replace("Default Title", "").strip(),
                    "Description": desc,
                    "Regular price": usd_price,
                    "Categories": category,
                    "Images": variant_image,
                    "cf_opingts": "",
                    "自定义分类": self.custom_category,
                    "原站域名": self.domain.split("//")[1],
                    "分布网站识别": 0,
                    "语言": "en",
                }

        if len(products) == self.limit:
            self.page += 1
            yield from self.request_page()
        else:
            self.logger.info("已到最后一页，抓取结束")

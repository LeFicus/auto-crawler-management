# pipelines.py
import pandas as pd
import os
from scrapy.exceptions import NotConfigured, CloseSpider

class PandasExporter:
    def __init__(self, file_name, fields):
        self.file_name = os.path.abspath(file_name)      # 绝对路径，日志好看
        self.fields = fields
        self.items = []                                  # 所有数据都攒在这里

    @classmethod
    def from_crawler(cls, crawler):
        file_name = crawler.settings.get("PANDAS_EXPORT_FILE", "ss.xlsx")
        fields = crawler.settings.get("PANDAS_FIELDS")
        if not file_name or not fields:
            raise NotConfigured("settings里没配置 PANDAS_EXPORT_FILE 或 PANDAS_FIELDS")
        return cls(file_name, fields)

    def open_spider(self, spider):
        self.file_name = spider.export_file
    def process_item(self, item, spider):
        # 只保留我们关心的字段 + 转成普通 dict
        row = {k: item.get(k, "") for k in self.fields}
        self.items.append(row)

        # 进度提示
        count = len(self.items)
        if count % 100 == 0:
            spider.logger.info(f"已缓存 {count} 条数据到内存，待导出...")
        return item

    def close_spider(self, spider):
        if not self.items:
            spider.logger.info("没有抓到任何数据，跳过导出")
            return

        try:
            df = pd.DataFrame(self.items)
            for field in self.fields:
                if field not in df.columns:
                    df[field] = ""
            df = df[self.fields]
            df = df.drop_duplicates(subset=["SKU"], keep="first")

            # 确保目录存在
            os.makedirs(os.path.dirname(self.file_name) or '.', exist_ok=True)

            # 最简单、最稳定、无兼容性问题的写法
            with pd.ExcelWriter(self.file_name, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='商品数据', index=False)

            spider.logger.info(f"成功导出 {len(df)} 条数据 → {self.file_name}")

        except Exception as e:
            spider.logger.error(f"导出彻底失败：{e}")
            raise CloseSpider(f"Excel导出失败：{e}")
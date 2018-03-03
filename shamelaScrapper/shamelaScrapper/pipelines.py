# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import apsw

TABLE_SHAMELA_OFFICIAL = "books_shamela_official"
TABLE_SHAMELA_REP = "books_shamela_rep"


class SQLiteInsertPipeline(object):
    collection_name = 'scrapy_items'

    def __init__(self, db_file):
        self.db_file = db_file

    @classmethod
    def from_crawler(cls, crawler):
        return cls(db_file=crawler.settings.get('SQLITE_PATH', 'data.sqlite'))

    def open_spider(self, spider):
        self.connection = apsw.Connection(self.db_file)
        self.cursor = self.connection.cursor()
        self.cursor.execute('CREATE TABLE IF NOT EXISTS ' + TABLE_SHAMELA_OFFICIAL +
                            '(id INTEGER NOT NULL PRIMARY KEY,'
                            'view_count INTEGER,'
                            'date_added TEXT,'
                            'tags TEXT,'
                            'rar_link TEXT,'
                            'pdf_link TEXT,'
                            'online_link TEXT,'
                            'epub_link TEXT)')
        self.cursor.execute('CREATE TABLE IF NOT EXISTS ' + TABLE_SHAMELA_REP +
                            '(id INTEGER NOT NULL PRIMARY KEY,'
                            'view_count INTEGER,'
                            'date_added TEXT,'
                            'rar_link TEXT,'
                            'uploading_user TEXT)')

    def close_spider(self, spider):
        # self.cursor.close()
        # self.connection.close(True)
        pass

    def process_item(self, item, spider):
        if item['repository'] == 'shamela.ws/index.php':
            self.cursor.execute("INSERT OR REPLACE INTO " + TABLE_SHAMELA_OFFICIAL + " values("
                                                                                     ":id,"
                                                                                     ":view_count,"
                                                                                     ":date_added,"
                                                                                     ":tags,"
                                                                                     ":rar_link,"
                                                                                     ":pdf_link,"
                                                                                     ":online_link,"
                                                                                     ":epub_link)"
                                , dict(item))
        elif item['repository'] == 'shamela.ws/rep.php':
            self.cursor.execute("INSERT OR REPLACE INTO " + TABLE_SHAMELA_REP + " values("
                                                                                ":id,"
                                                                                ":view_count,"
                                                                                ":date_added,"
                                                                                ":rar_link,"
                                                                                ":uploading_user)"
                                , dict(item))
        return item

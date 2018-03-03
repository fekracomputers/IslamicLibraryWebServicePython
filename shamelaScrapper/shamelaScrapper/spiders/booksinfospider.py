import datetime
import re
from urllib.parse import urljoin

import scrapy

from shamelaScrapper.items import ShamelaOnlineBookInfo


class BooksInfoSpider(scrapy.Spider):
    name = 'books_info'

    def start_requests(self):
        urls = [
            'http://shamela.ws/index.php/search/last/page-1/',
            'http://shamela.ws/rep.php/search/last/page-1'
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        for book in response.css('td.regular-book'):
            # read addition date
            yield response.follow(book.xpath('a/@href').extract_first(), self.parse_book)

        if self.folow_next:
            next_page = response.xpath("//a[text()='التالي']/@href").extract_first()
            if next_page is not None:
                yield response.follow(next_page, callback=self.parse)

    def parse_book(selfselfe, response):
        def select_info_desc_text(info_title):
            return response \
                .xpath("//span[@class='info-item']"
                       "/span[@class='info-title'][contains(text(),'%s')]"
                       "/following-sibling::span/text()" % info_title)

        def select_info_desc_href(info_title):
            return response \
                .xpath("//span[@class='info-item']"
                       "/span[@class='info-title'][contains(text(),'%s')]"
                       "/following-sibling::span/"
                       "a/"
                       "@href" % info_title)

        def select_link_from_img(img_src):
            raw_link = response.xpath(
                # "//div[@style='"
                # "text-align:center;"
                # "letter-spacing:"
                # " 25px;margin:"
                # " 20px 0;']"
                # "/a"
                "//img[contains(@src,'%s')]"
                "/parent::a"
                "/@href" % img_src).extract_first()
            return urljoin(response.url, raw_link) if raw_link else None

        def getRepositoryFromResponse(url):
            return "/".join(url.split('/')[2:4])

        book = ShamelaOnlineBookInfo()
        book['id'] = int(response.url.split('/')[-1])
        book['view_count'] = int(select_info_desc_text('عدد المشاهدات').extract_first())
        book['date_added'] = parse_date(select_info_desc_text('تاريخ الإضافة').extract_first())
        book['tags'] = ','.join(
            [*map(lambda url: (urljoin(response.url, url)) if url else None, select_info_desc_href('الوسوم').extract())])
        book['rar_link'] = select_link_from_img('bok.png')
        book['pdf_link'] = select_link_from_img('pdf.png')
        book['online_link'] = select_link_from_img('online.png')
        book['epub_link'] = select_link_from_img('epubd.png')
        book['uploading_user'] = urljoin(response.url,
                                         response.xpath("//a[contains(@href,'user')]/@href")
                                         .extract_first())
        book['repository'] = getRepositoryFromResponse(response.url)
        yield book


arabic_month_names = [None, 'يناير',
                      'فبراير',
                      'مارس',
                      'إبريل',
                      'مايو',
                      'يونيو',
                      'يوليو',
                      'أغسطس',
                      'سبتمبر',
                      'أكتوبر',
                      'نوفمبر',
                      'ديسمبر'
                      ]
prog = re.compile(r'(\s*\d{1,2})\s+(\D+)\s+(\d{4})\s+م?\s*')


def parse_date(date):
    m = prog.match(date)
    if (m):
        day = int(m.group(1))
        monthText = m.group(2)
        if monthText in arabic_month_names:
            monthNumber = arabic_month_names.index(monthText)
        else:
            raise ValueError('Invalid month name %s' % monthText)
        year = int(m.group(3))
        return datetime.date(year, monthNumber, day).strftime('%Y-%m-%d')
    else:
        raise ValueError('Invalid date format %s' % date)

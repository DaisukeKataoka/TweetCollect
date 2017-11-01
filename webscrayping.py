import urllib
import MeCab
from bs4 import BeautifulSoup
from lxml.html import fromstring
from Utils.preprocesssing import morphological_analyze


class Scrayping():
    def __init__(self, urls):
        self.urls = urls

    def get_html(self, url):
        html = urllib.request.urlopen(url)
        soup = BeautifulSoup(html, "lxml")
        # print(soup.body.contents)

        et = fromstring(str(soup))
        xpath = r'//text()[name(..)!="script"][name(..)!="style"]'
        text = ''.join([text for text in et.xpath(xpath) if text.strip()])
        # print(text)
        return text

    def preprocess(self):
        pass

def main(urls):
    sc = Scrayping(urls)
    sc.get_html(urls[0])

if __name__ == "__main__":
    urls = [
            "https://ja.wikipedia.org/wiki/%E5%9C%A8%E7%B1%8D%E8%80%85_(%E5%AD%A6%E7%BF%92%E8%80%85)",
            "http://dic.nicovideo.jp/a/%E5%AD%A6%E7%94%9F"
            ]
    main(urls)

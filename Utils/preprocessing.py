# -*- coding: utf-8 -*-

import MeCab

def morphological_analyze(text):
    m = MeCab.Tagger("-Ochasen")
    m.parse('') 
    return m.parse(text)

if __name__ == "__main__":
    txt = u"私は研究をしています"
    print(morphological_analyze(txt))
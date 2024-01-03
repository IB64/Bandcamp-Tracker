import spacy
import pandas as pd

DNB = ['Drum & Bass', 'Dnb', 'Drum N Bass']
RNB = ['Rnb', 'R&B']


def clean_tags(x):
    print(x)
    s = set()
    for tag in x:
        if '-' in tag:
            tag = tag.replace('-', ' ')
        if '/' in tag:
            tags = tag.split('/')
            for extra_tag in tags:
                s.add(extra_tag.title())
            continue
        if tag[-1] == '.':
            tag = tag[:-1]
        if tag.title() in DNB:
            s.add('DNB')
        elif tag.title in RNB:
            s.add('R&B')
        else:
            s.add(tag.title())
    print(s)
    return list(s)

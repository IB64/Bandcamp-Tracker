"""
Script to clean and transform all the data from the extract script.
"""
import pandas as pd
import spacy

DNB = ['Drum & Bass', 'Dnb', 'Drum N Bass']
RNB = ['Rnb', 'R&B']
FEATURING = ["ft.", "featuring"]
NLP_MODEL = spacy.load("en_core_web_sm")
EXTENDED_ASCII_RANGE = 255


def convert_to_df(extracted_data: list[dict]):
    """
    Converts a list of dictionaries into a pandas dataframe and returns it.
    """
    return pd.DataFrame(extracted_data)


def has_special_characters(name: str) -> bool:
    """
    If a given name has any special characters outside English, then return True
    """
    for character in name:
        if ord(character) > EXTENDED_ASCII_RANGE:
            return True
    return False


def clean_tags(tags: list[str]) -> list[str]:
    """
    Cleans the tags associated with the album / track.
    """
    if not tags:
        return ["Other"]
    tags_set = set()
    for tag in tags:
        doc = NLP_MODEL(tag)
        for ent in doc.ents:
            if ent.label_ in ("GPE", "PERSON"):
                break
        else:
            if tag == "":
                continue
            if has_special_characters(tag):
                continue
            if '/' in tag:
                tags = tag.split('/')
                for extra_tag in tags:
                    tags_set.add(extra_tag.title())
                continue
            if '-' in tag:
                tag = tag.replace('-', ' ')
            if tag[-1] == '.':
                tag = tag[:-1]
            if tag.title() in DNB:
                tags_set.add('DNB')
            elif tag.title() in RNB:
                tags_set.add('R&B')
            else:
                tags_set.add(tag.title())
    new_tags = list(tags_set)
    if new_tags:
        return new_tags
    return ["Other"]


def clean_artists(name: str) -> str:
    """
    For artist names that feature other artists,
    featured artists are erased and only the main artist is left.
    If special characters are detected, then change to "na"
    """
    if has_special_characters(name):
        return pd.NaT

    for word in FEATURING:
        if word in name:
            artists = name.split(f" {word}")
            return artists[0]
    return name


def clean_titles(name: str) -> str:
    """
    Clean the title. Removes any whitespace and new lines.
    If special characters are detected, then change to "na"
    """
    if has_special_characters(name):
        return pd.NaT
    return name.strip()


def clean_dataframe(dataframe: pd.DataFrame, sales_event: bool) -> pd.DataFrame:
    """
    Cleans the tags, the title, the amount paid for the album / track
    and the artist.
    """
    dataframe['tags'] = dataframe['tags'].apply(clean_tags)

    dataframe['title'] = dataframe['title'].apply(clean_titles)

    dataframe['amount_paid_usd'] = dataframe['amount_paid_usd'] * 100
    dataframe['amount_paid_usd'] = dataframe['amount_paid_usd'].astype(int)

    dataframe['artist'] = dataframe['artist'].apply(clean_artists)

    dataframe = dataframe.dropna()

    if sales_event:
        dataframe['amount_paid_usd'] = dataframe['amount_paid_usd'] / 100
        dataframe['amount_paid_usd'] = dataframe['amount_paid_usd'].astype(int)
        return dataframe

    dataframe = dataframe.explode('tags')

    return dataframe

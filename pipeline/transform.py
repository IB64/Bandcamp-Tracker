"""
Script to clean and transform all the data from the extract script.
"""
import pandas as pd
import spacy

DNB = ['Drum & Bass', 'Dnb', 'Drum N Bass']
RNB = ['Rnb', 'R&B']
FEATURING = ["ft.", "featuring"]
NLP_MODEL = spacy.load("en_core_web_sm")


def convert_to_df(extracted_data: list[dict]):
    """
    Converts a list of dictionaries into a pandas dataframe and returns it.
    """
    return pd.DataFrame(extracted_data)


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
            if ent.label_ == "GPE" or ent.label_ == "PERSON":
                break
        else:
            if tag == "":
                continue
            if '-' in tag:
                tag = tag.replace('-', ' ')
            if '/' in tag:
                tags = tag.split('/')
                for extra_tag in tags:
                    tags_set.add(extra_tag.title())
                continue
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
    else:
        return ["Other"]


def clean_artists(name: str) -> str:
    """
    For artist names that feature other artists,
    featured artists are erased and only the main artist is left
    """
    for word in FEATURING:
        if word in name:
            artists = name.split(f" {word}")
            return artists[0]
    return name


def clean_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the tags, the title, the amount paid for the album / track
    and the artist.
    """
    dataframe['tags'] = dataframe['tags'].apply(clean_tags)

    dataframe['title'] = dataframe['title'].str.strip()

    dataframe['amount_paid_usd'] = dataframe['amount_paid_usd'] * 100
    dataframe['amount_paid_usd'] = dataframe['amount_paid_usd'].astype(int)

    dataframe['artist'] = dataframe['artist'].apply(clean_artists)

    dataframe = dataframe.explode('tags')

    return dataframe

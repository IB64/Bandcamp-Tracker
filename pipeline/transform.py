"""
Script to clean and transform all the data from the extract script.
"""
import pandas as pd

DNB = ['Drum & Bass', 'Dnb', 'Drum N Bass']
RNB = ['Rnb', 'R&B']


def convert_to_df(extracted_data: list[dict]):
    """
    Converts a list of dictionaries into a pandas dataframe and returns it.
    """
    return pd.DataFrame(extracted_data)


def clean_tags(tags: list[str]) -> list[str]:
    """
    Cleans the tags associated with the album / track.
    """
    tags_set = set()
    for tag in tags:
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
        elif tag.title in RNB:
            tags_set.add('R&B')
        else:
            tags_set.add(tag.title())
    return list(tags_set)


def clean_artists(name: str) -> str:
    """
    For artist names that feature other artists,
    featured artists are erased and only the main artist is left
    """
    artists = name.split("ft.")
    return artists[0]


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

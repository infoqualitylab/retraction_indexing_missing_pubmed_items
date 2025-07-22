"""
This file contains methods to clean and filter the PubMed unionlist for analysis.

Functions overview:
convert_unicode: parses a string through various Unicode encoding options
read_in_unionlist: reads unionlist from csv file to Pandas DataFrame.
filter_for_2025_query: filters main unionlist to get only items returned in the 2025 query
clean_and_filter_publication_date: cleans and filters unionlist for items published between 1 Jan 2020 and 3 July 2024.
clean_and_filter_retraction_notice_date: cleans and filters for items with retraction notices published on or
before 3 Jul 2024
main: runs full script to filter and clean unionlist
"""
import numpy as np
import unicodedata
import pandas as pd
from datetime import date


def convert_unicode(string: str) -> str:
    """
    It takes a string and passes it through different encoding parameter phases
    E.g. '10.\u200b1105/\u200btpc.\u200b010357' ->  '10.1105/tpc.010357'

    :param string: variable to be encoded
    :return: the actual string value devoided of encoded character
    """

    string = unicodedata.normalize('NFKD', string).encode('iso-8859-1', 'ignore').decode('iso-8859-1')
    string = unicodedata.normalize('NFKD', string).encode('latin1', 'ignore').decode('latin1')
    string = unicodedata.normalize('NFKD', string).encode('cp1252', 'ignore').decode('cp1252')
    return string


def read_in_unionlist(pubmed_date: str):
    """
    Reads unionlist from csv file to Pandas DataFrame.
    :param pubmed_date: string of date unionlist was created
    :return: Pandas DataFrame of unionlist
    """

    unionlist = pd.read_csv(f"../data/2_{pubmed_date}_pubmed_unionlist.csv")
    # Remove csv artifact
    unionlist = unionlist.loc[:, ~unionlist.columns.str.contains('^Unnamed')]

    # Convert DOI information from unicode
    unionlist['DOI'] = unionlist['DOI'].str.lower().str.strip().astype(str).apply(convert_unicode)

    # Fill NA PubMed ID cells with 0, replace with empty string, make all PubMed IDs strings
    unionlist['PubMedID'] = unionlist['PubMedID'].fillna(0).astype(int).replace(0, '').astype(str).str.strip()

    # Fill NA Retraction Notice PubMed ID cells with 0, replace with empty string, make all PubMed IDs strings
    unionlist['Retraction_Notice_PubMedID'] = (unionlist['Retraction_Notice_PubMedID'].fillna(0).astype(int)
                                               .replace(0, '').astype(str))

    return unionlist


def filter_for_2025_query(unionlist: pd.DataFrame, pubmed_date: str):
    """
    Filters main unionlist to get only items returned in the 2025 query
    :param unionlist: unionlist dataframe
    :param pubmed_date: string of date unionlist was created
    :return: Pandas DataFrame of filtered unionlist
    """
    original_df = unionlist
    filtered_df = original_df[original_df['Indexed_as_retracted_in'] == '2025 query']
    filtered_df = filtered_df.loc[:, ~filtered_df.columns.str.contains('^Unnamed')]
    filtered_df.to_csv(f'../data/3_{pubmed_date}_pubmed_unionlist_2025_only.csv')

    return filtered_df


def clean_and_filter_publication_date(filtered_unionlist: pd.DataFrame, pubmed_date: str):
    """
    Cleans and filters for items published between 1 Jan 2020 and 3 July 2024.
    :param filtered_unionlist: unionlist dataframe including only items returned in the 2025 query
    :param pubmed_date: string of date unionlist was created
    :return: Pandas DataFrame of filtered unionlist
    """

    df = filtered_unionlist

    df[['Pub_Year', 'Pub_Month', 'Pub_Day']] = df.Publication_Date.str.split(pat=':', expand=True)

    month_dictionary = {
        'Jan': '1',
        'Feb': '2',
        'Mar': '3',
        'Apr': '4',
        'May': '5',
        'Jun': '6',
        'Jul': '7',
        'Aug': '8',
        'Sep': '9',
        'Oct': '10',
        'Nov': '11',
        'Dec': '12',
        '99': '1'
    }

    df['Pub_Month'] = df['Pub_Month'].replace(month_dictionary.keys(), month_dictionary.values())
    df['Pub_Day'] = df['Pub_Day'].replace('99', '01')

    df.to_csv(f'../data/4_{pubmed_date}_pubmed_unionlist_pub_date_cleaned.csv')

    df['Pub_Year'] = df['Pub_Year'].astype(str)
    df['Pub_Month'] = df['Pub_Month'].astype(str)
    df['Pub_Day'] = df['Pub_Day'].astype(str)

    df['Pandas_Pub_Date'] = df['Pub_Year'] + '-' + df['Pub_Month'] + '-' + df['Pub_Day']
    df['Pandas_Pub_Date'] = pd.to_datetime(df['Pandas_Pub_Date'], format='%Y-%m-%d')

    filtered_df = df.loc[(df['Pandas_Pub_Date'] >= '2020-01-01') &
                         (df['Pandas_Pub_Date'] <= '2024-07-03')]

    filtered_df = filtered_df.loc[:, ~filtered_df.columns.str.contains('^Unnamed')]

    filtered_df.to_csv(f'../data/5_{pubmed_date}_pubmed_unionlist_pub_date_filtered.csv')

    return filtered_df


def clean_and_filter_retraction_notice_date(filtered_unionlist: pd.DataFrame, pubmed_date: str):
    """
    Cleans and filters for items with retraction notices published on or before 3 Jul 2024
    :param filtered_unionlist: unionlist dataframe including only items returned in the 2025 query and published
    between 1 Jan 2020 and 3 July 2024
    :param pubmed_date: string of date unionlist was created
    :return: Pandas DataFrame of filtered unionlist
    """
    df = filtered_unionlist

    df[['Retraction_Notice_Year', 'Retraction_Notice_Month', 'Retraction_Notice_Day']] = \
        df.Retraction_Notice_Citation.str.extract(pat=r'\. (\d{4}) ?(\w{3})? ?(\d+)?', expand=True)

    month_dictionary = {
        'Jan': '1',
        'Feb': '2',
        'Mar': '3',
        'Apr': '4',
        'May': '5',
        'Jun': '6',
        'Jul': '7',
        'Aug': '8',
        'Sep': '9',
        'Oct': '10',
        'Nov': '11',
        'Dec': '12',
        '99': '1'
    }

    df['Retraction_Notice_Month'] = df['Retraction_Notice_Month'].replace(month_dictionary.keys(), month_dictionary.values())

    df['Retraction_Notice_Year'] = df['Retraction_Notice_Year'].fillna('1678')
    df['Retraction_Notice_Month'] = df['Retraction_Notice_Month'].fillna('1')
    df['Retraction_Notice_Day'] = df['Retraction_Notice_Day'].fillna('1')

    df.to_csv(f'../data/6_{pubmed_date}_pubmed_unionlist_retraction_notice_cleaned.csv')

    df['Retraction_Notice_Year'] = df['Retraction_Notice_Year'].astype(str)
    df['Retraction_Notice_Month'] = df['Retraction_Notice_Month'].astype(str)
    df['Retraction_Notice_Day'] = df['Retraction_Notice_Day'].astype(str)

    df['Pandas_Retraction_Notice_Date'] = df['Retraction_Notice_Year'] + '-' + df['Retraction_Notice_Month'] + '-' + df['Retraction_Notice_Day']
    df['Pandas_Retraction_Notice_Date'] = pd.to_datetime(df['Pandas_Retraction_Notice_Date'], format='%Y-%m-%d')

    filtered_df = df.loc[df['Pandas_Retraction_Notice_Date'] <= '2024-07-03']

    filtered_df = filtered_df.loc[:, ~filtered_df.columns.str.contains('^Unnamed')]

    filtered_df.to_csv(f'../data/7_{pubmed_date}_pubmed_unionlist_retraction_notice_filtered.csv')


def main():
    print("Cleaning and filtering the unionlist...")
    date_for_csv_save = "2025-05-09"
    original_unionlist = read_in_unionlist(pubmed_date=date_for_csv_save)

    items_only_in_2025_query = filter_for_2025_query(unionlist=original_unionlist, pubmed_date=date_for_csv_save)

    items_filtered_on_publication_year = (
        clean_and_filter_publication_date(filtered_unionlist=items_only_in_2025_query,
                                          pubmed_date=date_for_csv_save)
    )

    clean_and_filter_retraction_notice_date(
                                            filtered_unionlist=items_filtered_on_publication_year,
                                            pubmed_date=date_for_csv_save)

    print("Done!")


if __name__ == "__main__":
    main()

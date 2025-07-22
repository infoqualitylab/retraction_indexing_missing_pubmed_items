"""
This file contains methods to further analyze the PubMed unionlist.

Functions overview:
convert_unicode: parses a string through various Unicode encoding options
read_in_filtered_unionlist: reads filtered unionlist from csv file to Pandas DataFrame.
filter_for_retraction_notice_doi: further filters items in 7_{date}_pubmed_unionlist_retraction_notice_filtered to select
records that have a DOI as their retraction notice citation.
compare_two_runs: compares filtered results from different PubMed query dates, i.e. comparing 7_2025-05-09_pubmed_unionlist_retraction_notice_filtered.csv
and 7_2025-05-12_pubmed_unionlist_retraction_notice_filtered.csv
compare_to_full_unionlist: conpares filtered results to full unionlist used in "Analyzing the consistency of retraction
indexing".
"""
import unicodedata
import pandas as pd
from yaml import full_load


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


def read_in_filtered_pubmed_unionlist(pubmed_date: str):
    """
    Reads filtered unionlist from csv file to Pandas DataFrame.
    :param pubmed_date: string of date unionlist was created
    :return: Pandas DataFrame of unionlist
    """

    unionlist = pd.read_csv(f"../data/7_{pubmed_date}_pubmed_unionlist_retraction_notice_filtered.csv")

    # Convert DOI information from unicode
    unionlist['DOI'] = unionlist['DOI'].str.lower().str.strip().astype(str).apply(convert_unicode)

    # Fill NA PubMed ID cells with 0, replace with empty string, make all PubMed IDs strings
    unionlist['PubMedID'] = unionlist['PubMedID'].fillna(0).astype(int).replace(0, '').astype(str).str.strip()

    # Fill NA Retraction Notice PubMed ID cells with 0, replace with empty string, make all PubMed IDs strings
    unionlist['Retraction_Notice_PubMedID'] = (unionlist['Retraction_Notice_PubMedID'].fillna(0).astype(int)
                                               .replace(0, '').astype(str))

    # Read in dates as pd.datetime value
    unionlist['Pandas_Retraction_Notice_Date'] = pd.to_datetime(
        unionlist['Pandas_Retraction_Notice_Date'],
        format='%Y-%m-%d'
    )

    unionlist['Pandas_Pub_Date'] = pd.to_datetime(
        unionlist['Pandas_Pub_Date'],
        format='%Y-%m-%d'
    )

    unionlist = unionlist.loc[:, ~unionlist.columns.str.contains('^Unnamed')]

    return unionlist


def filter_for_retraction_notice_doi(filtered_unionlist: pd.DataFrame, pubmed_date: str):
    """
    Filters the current unionlist for retraction notices that have a DOI in the Retraction_Notice_Citation
    column.
    :param filtered_unionlist: unionlist dataframe including only items returned in the 2025 query, published
    between 1 Jan 2020 and 3 July 2024, and with a retraction notice published on or before 3 Jul 2024.
    :param pubmed_date: string of date unionlist was created
    :return:
    """
    df = filtered_unionlist

    # Filter for items with a retraction notice DOI (starts with '10.' for the citation).
    retraction_notice_doi_df = df[(df['Retraction_Notice_Citation'].str.contains(r'^10\.'))]

    # Further filter for items with a sentinel value as the retraction notice date
    retraction_notice_doi_df = retraction_notice_doi_df[
        retraction_notice_doi_df['Pandas_Retraction_Notice_Date'] == pd.to_datetime('1678-01-01')
        ]

    # Make Retraction Notice DOI column
    retraction_notice_doi_df['Retraction_Notice_DOI'] = retraction_notice_doi_df['Retraction_Notice_Citation']

    # Check if listed Retraction Notice DOI is the same as the original publication DOI
    retraction_notice_doi_df["Same_DOI_for_notice_and_publication"] = (
        retraction_notice_doi_df.apply(lambda row: row.Retraction_Notice_DOI.str.lower() == row.DOI.str.lower(), axis=1)
    )
    retraction_notice_doi_df = retraction_notice_doi_df.loc[:,
                               ~retraction_notice_doi_df.columns.str.contains('^Unnamed')]

    retraction_notice_doi_df.to_csv(f'../data/8_{pubmed_date}_pubmed_unionlist_retraction_notice_with_doi.csv')

    return retraction_notice_doi_df


def compare_two_pubmed_runs(earlier_df_date: str, later_df_date: str):
    """
    Compares what records exist in two runs, a 'left run' and a 'right run' when merging
    :param earlier_df_date: date in title of csv file with earlier information
    :param later_df_date: date in title of csv file with later information
    :return: csv file with all information from both runs compared using PMID
    """
    earlier_run = read_in_filtered_pubmed_unionlist(pubmed_date=earlier_df_date)
    later_run = read_in_filtered_pubmed_unionlist(pubmed_date=later_df_date)

    merged_df_with_conflicts = pd.merge(earlier_run,
                                        later_run,
                                        on='PubMedID',
                                        how='outer',
                                        indicator=True)
    merged_df_with_conflicts.rename(columns={'_merge': 'Merge'}, inplace=True)
    merged_df_with_conflicts.replace('left_only', 'earlier_run', inplace=True)
    merged_df_with_conflicts.replace('right_only', 'later_run', inplace=True)

    merged_df_with_conflicts = merged_df_with_conflicts.loc[:,
                               ~merged_df_with_conflicts.columns.str.contains('^Unnamed')
                               ]

    merged_df_with_conflicts.to_csv(f'../data/9_filtered_merged_df_for_{earlier_df_date}_and_{later_df_date}.csv')


def compare_to_full_unionlist(pubmed_date: str):
    """
    Compares what records exist in the full unionlist used in "Analyzing the consistency of retraction indexing" vs what
    records exist in the filtered remaining PubMed items
    :param pubmed_date: date used to save file containing PubMed missing items
    :return: csv file with all information from both files compared using PMID
    """

    # Read in missing PubMed items
    missing_pubmed_items = pd.read_csv(f"../data/7_{pubmed_date}_pubmed_unionlist_retraction_notice_filtered.csv")
    # # Convert DOI information from unicode
    missing_pubmed_items['DOI'] = missing_pubmed_items['DOI'].str.lower().str.strip().astype(str).apply(convert_unicode)
    # # Fill NA PubMed ID cells with 0, replace with empty string, make all PubMed IDs strings
    missing_pubmed_items['PubMedID'] = missing_pubmed_items['PubMedID'].fillna(0).astype(int).replace(0, '').astype(str).str.strip()
    # # Fill NA Retraction Notice PubMed ID cells with 0, replace with empty string, make all PubMed IDs strings
    missing_pubmed_items['Retraction_Notice_PubMedID'] = (missing_pubmed_items['Retraction_Notice_PubMedID'].fillna(0).astype(int)
                                               .replace(0, '').astype(str))
    # # Read in dates as pd.datetime value
    missing_pubmed_items['Pandas_Retraction_Notice_Date'] = pd.to_datetime(
        missing_pubmed_items['Pandas_Retraction_Notice_Date'],
        format='%Y-%m-%d'
    )
    missing_pubmed_items['Pandas_Pub_Date'] = pd.to_datetime(
        missing_pubmed_items['Pandas_Pub_Date'],
        format='%Y-%m-%d'
    )
    # # Remove CSV reading artifact
    missing_pubmed_items = missing_pubmed_items.loc[:, ~missing_pubmed_items.columns.str.contains('^Unnamed')]

    # Read in completed union list
    full_unionlist = pd.read_csv(f"../data/unionlist_completed_ria_2024-07-09.csv")
    # # Convert DOI information from unicode
    full_unionlist['DOI'] = full_unionlist['DOI'].str.lower().str.strip().astype(str).apply(convert_unicode)
    # # Fill NA PubMed ID cells with 0, replace with empty string, make all PubMed IDs strings
    full_unionlist['PubMedID'] = full_unionlist['PubMedID'].fillna(0).astype(int).replace(0, '').astype(str).str.strip()
    # # Remove CSV reading artifact
    full_unionlist = full_unionlist.loc[:, ~full_unionlist.columns.str.contains('^Unnamed')]

    # Merge dataframes
    merged_df_with_conflicts = pd.merge(full_unionlist,
                                        missing_pubmed_items,
                                        on='PubMedID',
                                        how='outer',
                                        indicator=True)
    merged_df_with_conflicts.rename(columns={'_merge': 'Merge'}, inplace=True)
    merged_df_with_conflicts.replace('left_only', 'unionlist', inplace=True)
    merged_df_with_conflicts.replace('right_only', 'missing_pubmed_item', inplace=True)

    merged_df_with_conflicts['Coverage column source_new contains PubMed'] = (
        merged_df_with_conflicts['source_new'].str.contains('PubMed', na=False)
    )

    merged_df_with_conflicts['Indexing column source_old contains PubMed'] = (
        merged_df_with_conflicts['source_old'].str.contains('PubMed', na=False)
    )

    merged_df_with_conflicts['DOI same in full unionlist and in missing PubMed items'] = (
        merged_df_with_conflicts['DOI_x'].str.lower() == merged_df_with_conflicts['DOI_y'].str.lower()
    )

    merged_df_with_conflicts = merged_df_with_conflicts.loc[:, ~merged_df_with_conflicts.columns.str.contains('^Unnamed')]

    merged_df_with_conflicts.to_csv(f'../data/10_merged_df_for_full_unionlist_and_{pubmed_date}_filtered_missing_pubmed_items.csv')


def main():
    # print("Reading in PubMed unionlist...")
    # date_for_csv_save = "2025-05-09"
    # filtered_pubmed_unionlist = read_in_filtered_pubmed_unionlist(pubmed_date=date_for_csv_save)
    #
    # print("Filtering for retraction notices with DOIs and checking for matching DOIs...")
    # retraction_notice_df = filter_for_retraction_notice_doi(filtered_pubmed_unionlist, pubmed_date=date_for_csv_save)

    # print("Comparing two PubMed runs...")
    # compare_two_pubmed_runs(earlier_df_date='2025-05-09', later_df_date='2025-05-12')

    print("Comparing current filtered PubMed run to full unionlist...")
    compare_to_full_unionlist(pubmed_date='2025-05-09')

    print("Done!")


if __name__ == "__main__":
    main()

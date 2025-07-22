"""
This file contains methods to combine data from {date}_pubmed.csv into one unionlist.

Functions overview:
convert_unicode: parses a string through various Unicode encoding options
clean_pubmed_data: read previously-created csv file, create pandas dataframes, standardize missing values
create_pubmed_union_list: create union list and save to .csv file, matching on PMID values
main: runs full script to create union list, with variable parameters
"""
import unicodedata
import pandas as pd


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


def clean_pubmed_data(pubmed_date: str) -> pd.DataFrame:
    """
    Read in previously-gathered CSV file and return cleaned pandas dataframe for PubMed.

    :param pubmed_date: date that indexing information was gathered from PubMed
    :return: cleaned pandas dataframe
    """
    pubmed = pd.read_csv(f"../data/{pubmed_date}_pubmed.csv").rename(
        columns={'RetractionPubMedID': 'Retraction_Notice_PubMedID'})  # .drop(['Unnamed: 0'],axis=1

    # Add column for target indicator
    pubmed['Indexed_In'] = 'PubMed'

    # Extract date information and make an integer value
    pubmed['Date'] = pubmed['Year']

    # Convert DOI information from unicode
    pubmed['DOI'] = pubmed['DOI'].str.lower().str.strip().astype(str).apply(convert_unicode)

    # Fill NA PubMed ID cells with 0, replace with empty string, make all PubMed IDs strings
    pubmed['PubMedID'] = pubmed['PubMedID'].fillna(0).astype(int).replace(0, '').astype(str).str.strip()

    # Fill NA Retraction Notice PubMed ID cells with 0, replace with empty string, make all PubMed IDs strings
    pubmed['Retraction_Notice_PubMedID'] = (pubmed['Retraction_Notice_PubMedID'].fillna(0).astype(int)
                                            .replace(0, '').astype(str))

    return pubmed


def create_pubmed_union_list(pubmed_2024: pd.DataFrame, pubmed_2025: pd.DataFrame, date_for_save: str):
    """
    Creates union list from all sources, matching on DOI, and saves to .csv value
    :param pubmed_2024: pandas dataframe of PubMed information from 2024
    :param pubmed_2025: pandas dataframe of PubMed information from 2025
    :param date_for_save: date to add to saved union list csv file title
    :return: union list csv file
    """

    merged_df_with_conflicts = pd.merge(pubmed_2024,
                                        pubmed_2025,
                                        on='PubMedID',
                                        how='outer',
                                        indicator=True)
    merged_df_with_conflicts.rename(columns={'_merge': 'Merge'}, inplace=True)

    merged_df_with_conflicts = merged_df_with_conflicts.loc[:, ~merged_df_with_conflicts.columns.str.contains('^Unnamed')]
    merged_df_with_conflicts.to_csv(f'../data/1_{date_for_save}_merged_df.csv')

    # Set up DataFrame
    fused_df = pd.DataFrame(columns=["DOI",
                                     "Author",
                                     "Title",
                                     "Publication_Date",
                                     "Journal",
                                     "PubMedID",
                                     "Retraction_Notice_PubMedID",
                                     "Retraction_Notice_Citation",
                                     "Indexed_as_retracted_in",])

    # Merge data sources. In cases where conflicts exist,
    # PubMed 2024 data was used (row.COLUMN_x, left DataFrame in original merge).

    for row in merged_df_with_conflicts.itertuples():
        if row.Merge == 'both':
            new_row = {'DOI': row.DOI_x,
                       'Author': row.Author_x,
                       'Title': row.Title_x,
                       'Publication_Date': row.Date_x,
                       'Journal': row.Journal_x,
                       'PubMedID': row.PubMedID,
                       'Retraction_Notice_PubMedID': row.Retraction_Notice_PubMedID_x,
                       "Retraction_Notice_Citation": row.RetractionNotice_x,
                       "Indexed_as_retracted_in": "2024 query; 2025 query",}
        elif row.Merge == 'left_only':
            new_row = {'DOI': row.DOI_x,
                       'Author': row.Author_x,
                       'Title': row.Title_x,
                       'Publication_Date': row.Date_x,
                       'Journal': row.Journal_x,
                       'PubMedID': row.PubMedID,
                       'Retraction_Notice_PubMedID': row.Retraction_Notice_PubMedID_x,
                       "Retraction_Notice_Citation": row.RetractionNotice_x,
                       "Indexed_as_retracted_in": "2024 query",}
        elif row.Merge == 'right_only':
            new_row = {'DOI': row.DOI_y,
                       'Author': row.Author_y,
                       'Title': row.Title_y,
                       'Publication_Date': row.Date_y,
                       'Journal': row.Journal_y,
                       'PubMedID': row.PubMedID,
                       'Retraction_Notice_PubMedID': row.Retraction_Notice_PubMedID_y,
                       "Retraction_Notice_Citation": row.RetractionNotice_y,
                       "Indexed_as_retracted_in": "2025 query",}
        fused_df.loc[len(fused_df)] = new_row

    # Remove NA values from PubMedID column
    fused_df['PubMedID'] = fused_df['PubMedID'].fillna(0).astype(int).replace(0, '').astype(str)
    fused_df = fused_df.loc[:, ~fused_df.columns.str.contains('^Unnamed')]

    fused_df.to_csv(f'../data/2_{date_for_save}_pubmed_unionlist.csv')


def main():
    print("Reading in datasets...")
    date_of_comparison_pubmed_run = '2025-05-09'
    pubmed_2024 = clean_pubmed_data(pubmed_date='2024-07-03')
    pubmed_2025 = clean_pubmed_data(pubmed_date=date_of_comparison_pubmed_run)

    print("2024 PubMed data:")
    print(pubmed_2024.head())
    print(f"shape: {pubmed_2024.shape}\n")
    print("2025 PubMed data:")
    print(pubmed_2025.head())
    print(f"shape: {pubmed_2025.shape}\n")

    print("Merging datasets...")
    create_pubmed_union_list(pubmed_2024=pubmed_2024,
                             pubmed_2025=pubmed_2025,
                             date_for_save=date_of_comparison_pubmed_run)

    print("Done!")


if __name__ == '__main__':
    main()

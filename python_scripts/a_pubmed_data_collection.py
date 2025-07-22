"""
This file contains methods to collect information about retracted items from PubMed.

Functions overview:
fetch_all_pmids(): retrieves all PMIDs for a given search term by batching
    Uses sub-function retrieve_pmids
retrieve_pmids(): queries PubMed for PMIDs of a given search term in a given year range
batch_pmids(): cuts list of PMIDs into batches
retrieve_xml_data_from_metadata(): queries PubMed for XML data for a given batch of PMIDs
extract_retracted_paper_metadata(): extracts desired information from XML for a given PMID
    Uses sub-functions get_authors_detail and get_retraction_notice
get_authors_detail(): extracts authors' names and affiliations
get_retraction_notice(): extracts journal title, retraction notice date, and retraction notice PMID
get_pubmed_data: extracts all retracted publication information from PubMed and saves it to csv.
    Parameters can be varied.
main(): runs full script to create PubMed csv file, with variable parameters
"""
import csv
import requests
import time
from bs4 import BeautifulSoup as bs
from datetime import date
from tqdm import tqdm


def fetch_all_pmids(term: str, start_year: int, end_year: int, interval_year: int, email: str) -> tuple:
    """
    It retrieves PMIDs for a given search term over a period of time using retrieve_pmids sub-function.
    It re-iterates at a defined interval year (e.g. searching 2000-2004, 2005-2009, etc.) because up to 10,000 records
    maximum can be retrieved from PubMed at time. Check: https://www.ncbi.nlm.nih.gov/books/NBK25501/ for details.

    :param email: supplied email
    :param term: search term
    :param start_year: the year to start the search
    :param end_year: the year to end the search
    :param interval_year: the interval of years to search in

    :return: tuple of count and of all PMIDs of the records retrieved
    """
    all_pmids = []
    total_pmids_count = 0
    current_year = end_year

    # Iterate over the years with a stipulated year interval of 10,000 records maximum limitation
    for year in range(start_year, end_year + 1, interval_year):
        end_year = year + interval_year - 1
        if (current_year - year) < interval_year:
            end_year = current_year

        count, pmids_per_interval = retrieve_pmids(term, year, end_year, email)
        time.sleep(1)
        total_pmids_count += count
        all_pmids += pmids_per_interval

        print(f'{year} - {end_year}: {count} total number of retrieved PMIDs')

    return total_pmids_count, all_pmids


def retrieve_pmids(term: str, mindate: int, maxdate: int, email: str) -> tuple:
    """
    It retrieves PMIDs for a given search term.

    :param email: supplied email
    :param term: search term
    :param mindate: the year to start the search
    :param maxdate: the year to end the search

    :return: tuple of count and of all PMIDs of the records retrieved
    """

    api_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"

    # Step 1: Search for retracted papers
    params = {
        "db": "pubmed",
        "term": term,
        "retmode": "json",
        "retmax": 10000,  # Maximum number of results per request
        "mindate": mindate,
        "maxdate": maxdate,
        "email": email,
    }

    # Send a GET request to the PubMed API to search for retracted papers
    response = requests.get(api_url, params=params)
    data = response.json()
    # print(data)
    total_results = int(data["esearchresult"]["count"])
    pmids = data["esearchresult"]["idlist"]

    return total_results, pmids


def batch_pmids(pmids: list, cut: int) -> list[list]:
    """
    It divides the list PMIDs into batches for processing.
    :param pmids: list of PMIDs
    :param cut: maximum number of records to assign to a batch

    :return: nested list of batches (lists) of PMIDs
    """
    pmids_batches = []

    while len(pmids) >= cut:
        selected_pmids = pmids[:cut]
        pmids_batches.append(selected_pmids)
        #         print(selected_pmids)
        pmids = pmids[cut:]

    if pmids:
        pmids_batches.append(pmids)
        #         print(pmids)

    return pmids_batches


def retrieve_xml_data_from_metadata(pmid: list, email: str) -> str:
    """
    It retrieves XML of a given list of PMIDs

    :param email: supplied email
    :param pmid: the list of PMIDs
    :return: XML of the list of PMIDs
    """

    # Manual example: https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id=37090826&retmode=xml
    efetch_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"

    # Retrieve paper's XML details
    params = {
        "db": "pubmed",
        "id": pmid,
        "retmode": "xml",
        "email": email,
    }

    # Process the XML response to extract the desired paper details
    # (e.g., title, authors, abstract, etc.)
    response = requests.get(efetch_url, params=params)
    paper_xml = response.text

    return paper_xml


def extract_retracted_paper_metadata(soup: bs) -> list:
    """
    It extracts data from the XML for retracted publications

    :param soup: article in Beautifulsoup XML format

    :return: list of extracted data from the XML metadata
    """
    # initialize all variables
    pmid, doi = '', ''  # retracted paper's pmid and doi
    pub_year, year, month, day = '', '9999', '99', '99'  # publication of the retracted paper
    jour_abrv, jour_title, title = '', '', ''  # journal title, abbreviation and article title of the retracted paper
    pub_type = ''  # publication type of the retracted paper such as letter, article etc.
    authors_names, authors_affils = '', ''  # authors' names & affiliations
    retraction_notice_detail, retraction_notice_pmid, retraction_of = None, None, None
    # Default value if attribute not found

    try:

        # extract pmid
        if soup.PMID:  # pmid
            pmid = soup.PMID.string
        elif soup.ArticleIdList.find(IdType="pubmed"):
            pmid = soup.ArticleIdList.find(IdType="pubmed").string
            # print(pmid)

            # extract doi
        if soup.ArticleIdList.find(IdType="doi") is not None:
            doi = soup.ArticleIdList.find(IdType="doi").string

            # fetching publication year
        if soup.ArticleDate:
            if soup.ArticleDate.Year is not None:
                year = soup.ArticleDate.Year.string

            if soup.ArticleDate.Month is not None:
                month = soup.ArticleDate.Month.string

            if soup.ArticleDate.Day is not None:
                day = soup.ArticleDate.Day.string

        elif soup.PubDate:
            if soup.PubDate.Year is not None:
                year = soup.PubDate.Year.string

            if soup.PubDate.Month is not None:
                month = soup.PubDate.Month.string

            if soup.PubDate.Day is not None:
                day = soup.PubDate.Day.string

        if year == '9999':
            """
            Loop through options where:
            <PubMedPubDate PubStatus="pubmed">
            <PubMedPubDate PubStatus="medline">
            <PubMedPubDate PubStatus="entrez">
            """

            if soup.find_all('PubMedPubDate', {'PubStatus': "pubmed"}):
                pub_date_elements = soup.find_all('PubMedPubDate', {'PubStatus': "pubmed"})
                for pub_date in pub_date_elements:
                    if pub_date.find("Year").text:
                        year = pub_date.find("Year").text
                    if pub_date.find("Month").text:
                        month = pub_date.find("Month").text
                    if pub_date.find("Day").text:
                        day = pub_date.find("Day").text

            elif soup.find_all('PubMedPubDate', {'PubStatus': "medline"}):
                pub_date_elements = soup.find_all('PubMedPubDate', {'PubStatus': "medline"})
                for pub_date in pub_date_elements:
                    if pub_date.find("Year").text:
                        year = pub_date.find("Year").text
                    if pub_date.find("Month").text:
                        month = pub_date.find("Month").text
                    if pub_date.find("Day").text:
                        day = pub_date.find("Day").text

            elif soup.find_all('PubMedPubDate', {'PubStatus': "entrez"}):
                pub_date_elements = soup.find_all('PubMedPubDate', {'PubStatus': "entrez"})
                for pub_date in pub_date_elements:
                    if pub_date.find("Year").text:
                        year = pub_date.find("Year").text
                    if pub_date.find("Month").text:
                        month = pub_date.find("Month").text
                    if pub_date.find("Day").text:
                        day = pub_date.find("Day").text

        pub_year = f'{year}:{month}:{day}'

        # extract title
        if soup.ArticleTitle is not None:
            title = soup.ArticleTitle.string
            # print(title)

        # extract journal title
        if soup.Title is not None:
            jour_title = soup.Title.string

        # extract journal title abbreviation
        if soup.ISOAbbreviation is not None:
            jour_abrv = soup.ISOAbbreviation.string

        # extract authors
        authorlist = soup.AuthorList
        if authorlist is not None:
            authors = soup.AuthorList.find_all('Author')

            # calling function to extract the authors names and affiliations
            authors_names, authors_affils = get_authors_detail(authors)

        # extract publication types
        if soup.PublicationTypeList is not None:
            pub_type = soup.PublicationTypeList.find_all()
            pub_type = ';'.join([pub.string for pub in pub_type])

        elif soup.PublicationType is not None:
            pub_type = soup.PublicationType.string

        # Call function to extract retraction notice
        retraction_notice_detail, retraction_notice_pmid = get_retraction_notice(soup)

        # Checking Attribute 'RetractionOf' to if the PMID is a retraction notice
        retraction_of = soup.find('CommentsCorrections', attrs={'RefType': 'RetractionOf'})
        if retraction_of is not None and retraction_of.PMID is not None:
            retraction_of = retraction_of.PMID.string

    except Exception:
        pass
        print(f'error at {pmid} with {doi}')

    return [pmid, doi, pub_year, authors_names, authors_affils, title, pub_type,
            jour_title, jour_abrv, retraction_notice_pmid, retraction_notice_detail, retraction_of]


def get_authors_detail(authors) -> tuple:
    """
    It extracts authors' names and their affiliations
    :param authors: authors' xml details - names and affiliations
    :return: tuple values of authors and their affiliations
    """
    au_name_list, au_affil_list = [], []
    au_names, au_affils = '', ''

    for author in authors:
        # get forename/firstname
        forename = ''
        fname = author.ForeName

        if fname is not None:
            forename = fname.string
        else:
            forename = 'unknown'

        # get lastname/surname
        lastname = ''
        lname = author.LastName

        if lname is not None:
            lastname = lname.string
        else:
            lastname = 'unknown'

        au_name = f'{forename} {lastname}'
        au_name_list.append(au_name)
        au_names = ';'.join(au_name_list)

        # get affiliation
        au_affil = ''
        if author.Affiliation is not None:
            au_affil = author.Affiliation.string
            if au_affil is not None:
                au_affil_list.append(au_affil)

    if au_affil_list:
        au_affils = ';'.join(au_affil_list)

    return au_names, au_affils


def get_retraction_notice(soup: bs):
    """
    It extracts journal title, retraction notice date, and retraction notice PMID

    :param soup: article in Beautifulsoup XML format
    :return: journal title & retraction notice date, and retraction notice PMID
    """
    retraction_notice_detail = None  # Default value if the attribute is not found
    retraction_notice_pmid = None  # Default value if the attribute is not found

    r_notice = soup.find_all('CommentsCorrections', attrs={'RefType': 'RetractionIn'})

    result = []
    pmid_text = ''

    # Getting the retraction notice details such date and PMID
    for retraction in r_notice:
        ref_source = retraction.find('RefSource')
        if ref_source is not None:
            ref_source_text = ref_source.text
            result.append(ref_source_text)

        pmid = retraction.find('PMID')
        if pmid is not None:
            pmid_text = pmid.text

    if (result is not None) or (len(result) != 0):
        retraction_notice_detail = ','.join(result)

    retraction_notice_pmid = pmid_text
    # print(retraction_notice_detail)
    return retraction_notice_detail, retraction_notice_pmid


def get_pubmed_data(start_year: int, end_year: int, interval_year: int, term: str, email: str, no_records: int):
    """
    It extracts all retracted publication information from PubMed.

    :param start_year: year to start search.
    :param end_year: year to end search.
    :param interval_year: interval of years to search over.
    :param term: specific search term, here limiting to retracted publications via "Retracted Publication[PT]"
    :param email: valid email to be included in API requests
    :param no_records: number of records to batch
    :return: csv file of PubMed articles saved in data folder
    """
    total_retracted_publications, retracted_paper_pmids = \
        fetch_all_pmids(term=term, start_year=start_year, end_year=end_year, interval_year=interval_year, email=email)

    print(f'The total number of retracted publications between {start_year} and {end_year} is '
          f'{total_retracted_publications} records in PubMed as of today {date.today()}.')
    print(f'After double check duplication, there are {len(set(retracted_paper_pmids))} records.')

    pmids_batches = batch_pmids(retracted_paper_pmids, no_records)

    print(f'The PMIDs are divided into {len(pmids_batches)} batches.')
    print(f'The complete PMID list is divided into lists each containing {no_records} records maximum.')

    header = ['PubMedID', 'DOI', 'Year', 'Author', 'Au_Affiliation', 'Title', 'PubType',
              'Journal', 'JournalAbrv', 'RetractionPubMedID', 'RetractionNotice', 'RetractionOf']

    outfile = open(f"../data/{str(date.today())}_pubmed.csv", "w", encoding="utf-8", newline="")
    csvout = csv.writer(outfile)
    csvout.writerow(header)

    result_per_paper = []
    count = 1

    for selected_pmids in tqdm(pmids_batches):
        all_results = []
        print(f'batch {count}/{len(pmids_batches)}: {len(selected_pmids)} records')

        retracted_papers_xml = retrieve_xml_data_from_metadata(pmid=selected_pmids, email=email)

        soup = bs(retracted_papers_xml, features='xml')
        # print(soup)
        papers_xml = soup.find_all('PubmedArticle')  # <PubmedArticle> vs <PubmedBookArticle>

        time.sleep(10)  # it sometimes failed at 8secs

        for per_paper_xml in papers_xml:
            result_per_paper = extract_retracted_paper_metadata(per_paper_xml)
            # print(result_per_paper)
            all_results.append(result_per_paper)
            # csvout.writerow(result_per_paper)

        csvout.writerows(all_results)
        count += 1

    outfile.close()

    print("Be sure to manually check some rows against PubMed database.")
    print(f"Also check that current number of publications for that search term matches: "
          f"{total_retracted_publications}")


def main():

    get_pubmed_data(
        start_year=1950,  # Via the PubMed interface, retracted publications start in 1951
        end_year=date.today().year,
        interval_year=2,  # Choose a year interval where there are not more than 10,000 results returned;
                          # PubMed can only return 10,000 results per request.
                          # In 2022 there were over 6,000 retractions, so current best practice is year interval of 2.
        term="'Retracted Publication'[PT]",
        email="INSERT_EMAIL_HERE",
        no_records=300
    )

    # Check totals by comparing against the regular PubMed interface for the search term:
    # https://pubmed.ncbi.nlm.nih.gov/?term="Retracted+Publication"[pt]


if __name__ == '__main__':
    main()

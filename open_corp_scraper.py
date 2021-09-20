import requests
import pandas as pd
import os
from lxml import html
import time
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import datetime
import calendar
import re
import data_process as dp 
from rapidfuzz import fuzz, process 


def clean_date(raw_date):
    month_to_num = {name: num for num, name in enumerate(calendar.month_abbr) if num}
    split_date = raw_date.split()

    split_date[1] = month_to_num[split_date[1][0:3]]
    date_nums = [int(num) for num in split_date]
    day, month, year = tuple(date_nums)
    cleaned_date = datetime.date(year, month, day).isoformat()
    return cleaned_date



base_url = 'https://opencorporates.com'
base_api_url = 'https://api.opencorporates.com'
headers = {
    'authority': 'opencorporates.com',
    'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Google Chrome";v="92"',
    'sec-ch-ua-mobile': '?0',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-user': '?1',
    'sec-fetch-dest': 'document',
    'accept-language': 'en-US,en;q=0.9',
    'cookie': '_gcl_au=1.1.815235363.1628965009; _ga=GA1.2.279465093.1628965010; survey_sparrow=kvKe3sKi1XMvvZWT; _gid=GA1.2.895445509.1629132973; _gat_UA-19844274-1=1; user_name=Jacob+Mobley; _openc_session=ZWFKRUh4UFdFekdBQ1ZVL2gxUW14ckU3QUlnUzBGQ2wrai93UnRwbGptdnJNYndjOFdxcURYY1ZZbjVmLzNaR0xtRzBXM2lBOXRhN1FSWkEraTAvc1JzdlNEQ2pybTQ5cWZteEI2WEUyRWc3ZEsrbTVMclRaY2dCcTEzbUNpM0VJNkNvSjY1YVU0ekVuU2hRWEdiUzJkZlRwRzZhYVdaZ0lCMVdaNEliZjFsaEpRZExCRURiOGRwNEdsUkhDM2dyRm5FWVl3dDhDSkRnU1RIZmRkTnFpNHBRVlZ2R2wwSjM5aW1BNkFrM0Qwa2NFMlNUMlNSWTBMRWUzK2h4eFY5NWI5L01FR1NwWk9YdERaWUw3cWN3YWc9PS0tc0RSeUxDOU5RL0tuVngwSEZYbEdpdz09--3b9ee75ebecebde61beb909de274faeb5344fda3',
}
def is_active(status):
        active_status = [
            'Active',
            'Good Standing',
            'Active And In Good Standing'
            ]
        if status in active_status:
            return True
        return False

def req_to_lxml_tree(url=None, response=None, ):
    '''Takes a url  or response and returns an lxml tree for parsing.'''
    if url:
        response = requests.get(url, stream=True, headers=headers)
    if response:
        response = response
    response.raw.decode_content = True
    tree = html.parse(response.raw)
    return tree



def get_urls(url, fuzzy_match='', follow=False, delay=0, page_limit=None):

    '''Takes an oc search result url and return all the links for that result.
    -------------------
    url => string : Start url.

    fuzzy_match => string : Whether to return all results from the search or only ones that meet a match threshold with a given query. 
    Useful for combining with dp.generate_search_url() while searching from a list of LLC names 'blind'.

    follow => bool: Whether to attempt to follow the next pagination page of results.

    delay => int : Number of seconds to wait between follow requests.

    page_limit => int: Number of pages to limit the pagination follower to.
    
    '''
    tree = req_to_lxml_tree(url)
    all_hrefs = tree.xpath('//a[contains(@class, "company")]/@href')
    if fuzzy_match:
        results = tree.xpath('//a[contains(@class, "company")]')
        fuzzy_match = dp.remove_symbols(fuzzy_match.lower())
        fuzzy_match_list = []
        for res in results:
            result_string = dp.remove_symbols(res.xpath('./text()')[0].lower())
            match_score = fuzz.WRatio(result_string,fuzzy_match)
            if match_score >= 99:
                fuzzy_match_list.append(res.xpath('./@href')[0])                
        all_hrefs = fuzzy_match_list
    urls = [base_url + h for h in all_hrefs]
    urls = list(set(urls))
    if follow:
        next_url = tree.xpath('//a[@rel="next nofollow"][contains(text(),"Next")]/@href')
        if len(next_url) > 0:
            next_url = next_url[0]
            page = int(re.search('page=(\d+)', next_url)[1])
            if page <= page_limit:
                next_url = base_url + next_url
                time.sleep(delay)
                urls.extend(get_urls(
                    next_url, 
                    fuzzy_match=fuzzy_match, 
                    follow=follow, 
                    delay=delay, 
                    page_limit=page_limit)
                    )
        
    return urls

def process_officers(tree):
    '''Takes in a lxml tree and returns a pandas DataFrame of the officers names, titles and oc_link'''
    officer_names = tree.xpath('//a[contains(@class, "officer")]/text()')
    officer_titles = [t.replace(',', '').strip() for t in tree.xpath(
        '//a[contains(@class, "officer")]/../text()')]
    officer_hrefs = tree.xpath('//a[contains(@class, "officer")]/@href')
    officer_urls = [base_url + href for href in officer_hrefs]
    officers = list(zip(officer_names, officer_titles, officer_urls))
    officers_df = pd.DataFrame(officers)
    officers_df.rename(columns={
        0: 'name',
        1: 'title',
        2: 'link'
    }, inplace=True)
    return officers_df

def process_filings(tree):
    '''Takes in a lxml tree and returns a pandas DataFrame with the filing data'''
    filing_descs = tree.xpath('//a[@class="filing"]/text()')
    filing_dates = [t.strip() for t in tree.xpath('//div[@class="filing"]/div/text()')]
    filing_dates = [clean_date(f) for f in filing_dates if f]
    filing_hrefs = tree.xpath('//a[@class="filing"]/@href')
    filing_urls = [base_url + f for f in filing_hrefs]
    filing_ids = []
    for index, f in enumerate(filing_urls):
        tmp = req_to_lxml_tree(f)
        filing_id = tmp.xpath(
            '//dd[@class="filing_number truncate"]/text()')
        try:
            filing_ids.append(filing_id[0])
        except IndexError:
            filing_ids.append('No ID Found')
        time.sleep(2)
        if index == 0:
            print("Processing...take your time, we don't want to get blocked!")
        if index == len(filing_urls) - 1:
            print("Done!")
    filing_data = list(
        zip(filing_descs, filing_dates, filing_urls, filing_ids))
    filing_df = pd.DataFrame(
        filing_data,
        columns=["description", 'date', 'filing_url', 'filing_id']
    )
    return filing_df

def process_events(tree):
    '''Takes in a lxml tree and collects event data into a pandas DataFrame.'''
    events_url = base_url + \
        tree.xpath('//div[@class="see-more"]/a/@href')[0]
    event_tree = req_to_lxml_tree(events_url)
    event_dates = event_tree.xpath('//div[@class="oc-events-timeline"]//dt/text()')
    events = event_tree.xpath('//div[@class="oc-events-timeline"]//a/text()')
    event_data = list(zip(event_dates, events))
    event_df = pd.DataFrame(event_data, columns=['event_date', 'event'])
    return event_df

def process_main_page(tree):

    def get_registry_data(main_data):
        try:
            main_data.update({'registry_page': tree.xpath(
                '//dd[@class="registry_page"]/a/@href')[0]})
        except IndexError:
            main_data.update({'registry_page': tree.xpath(
                '//div[@id="source"]//a[@class="url external"]/@href')[0]})

    def check_company_type(tree):
        type_check = tree.xpath('//dd[@class="company_type"]/text()')[0]
        for_profit = [
            'Limited Liability Company - Domestic',
            'KANSAS FOR PROFIT CORPORATION'
        ]
        non_profit = ['Nonprofit Corporation – Domestic']

        if type_check.lower() in (company.lower() for company in for_profit):
            return 'for_profit'
        else:
            return 'non_profit'
    
    def is_govt_supplier(tree):
        check = tree.xpath('///div[contains(@class, "government_approved_supplier")]//a/text()')
        if len(check) !=0 :
            return True
        return False
        

    company_name = tree.xpath('//h1[@itemprop="name"]/text()')[0].strip()
    print(f'Processing {company_name}...')
    # gather non-unique data
    company_number = tree.xpath('//dd[@class="company_number"]/text()')[0]
    status = tree.xpath('//dd[@class="status"]/text()')[0]
    inc_date = tree.xpath('//span[@itemprop="foundingDate"]/text()')[0]
    company_type = tree.xpath('//dd[@class="company_type"]/text()')[0]
    jurisdiction = tree.xpath('//a[@class="jurisdiction_filter us"]/text()')[0]
    registered_addr = ', '.join(tree.xpath('//dd[@class="registered_address adr"]//li/text()'))
    agent_name = tree.xpath('//dd[@class="agent_name"]/text()')
    previous_names = tree.xpath('//dd[@class="previous_names"]//li/text()')
    organizer_name = tree.xpath('//li[contains(text(),"organizer")]/a/text()')
    if len(organizer_name) == 0:
        organizer_name = "Not Found"
    if len(organizer_name) > 1:
        organizer_name = ','.join(organizer_name)
    else:
        organizer_name = organizer_name[0]
    #non unique data dictionary
    main_data = {
        'name': company_name,
        'company_number': company_number,
        'status': status,
        'inc_date': clean_date(inc_date),
        'company_type': company_type,
        'jurisdiction': jurisdiction,
        'registered_addr': registered_addr,
        'organizer_name': organizer_name
    }

    #gather unique elements if present for non_profit or for_profit companies
    status = main_data['status']
    if check_company_type(tree) == 'for_profit':
        business_desc = tree.xpath('//dd[@class="business_classification_text"]/text()')
        if is_active(status):
            main_data['agent_name'] = agent_name[0]
        if len(business_desc) == 0:
            business_desc = "No description found"
            main_data['business_desc'] = business_desc
        else:
            main_data['business_desc'] = business_desc[0]
        
    #check for non_profit data
    elif check_company_type(tree) == 'non_profit':
        agent_address = tree.xpath('//dd[@class="agent_address"]/text()')
        if len(agent_address) == 0:
            agent_address = "None Found"
            main_data['agent_address'] = agent_address
        else:
            main_data['agent_name'] = agent_name[0]
            main_data['agent_address'] = agent_address[0]
    #check if the company is still active
    if not is_active(status):

        dissolve_date = tree.xpath('//dd[contains(@class, "dissolution")]/text()')
        if len(dissolve_date) == 0:
            print(dissolve_date)
            main_data['dissolve_date'] = "Not Found"
        else:
            main_data['dissolve_date'] = clean_date(dissolve_date[0])

    #check for previous names of company
    if len(previous_names) == 1:
        main_data['previous_names'] = previous_names[0]
    if len(previous_names) > 1 and len(previous_names) != 0:
        main_data['previous_names'] = ', '.join(previous_names)
    
    #check if company is CAGE
    if is_govt_supplier(tree):
        x = tree.xpath
        govt_sup_addr = x('normalize-space(//a[contains(text(), "Company Address")]/../following-sibling::p/text())') 
        govt_sup_office_addr = x('normalize-space(//a[contains(text(), "Head Office Address")]/../following-sibling::p/text())')
        other_ids = x('//a[@class="identifier"]/text()')
        main_data['is_govt_supplier'] = "True"
        main_data['govt_sup_addr'] = govt_sup_addr
        main_data['govt_sup_office_addr'] = govt_sup_office_addr
        main_data['other_ids'] = other_ids[0]
        if len(other_ids) > 1:
            main_data['other_ids'] = ', '.join(other_ids)


    #check for registry url
    get_registry_data(main_data)

    

    #format data_frame
    main_data_df = pd.DataFrame([main_data], index=[1])
    main_data_df.rename({1: 'company_info'}, inplace=True)
    return main_data_df.transpose()

def process_api(url):
    '''Takes a company url and processes it returning a tuple with
        data, officer and filings DataFrames.'''
    response = requests.get(url)
    result = response.json()['results']['company']
    officers = result['officers']
    off_list = [o['officer'] for o in officers]
    filings = result['filings']
    filings_list = [f['filing'] for f in filings]
    filings_df = pd.DataFrame(filings_list)
    officers_df = pd.DataFrame(off_list)

    data_df = pd.json_normalize(result).transpose().drop(
        index=['filings', 'officers'])
    data_df = data_df.loc[data_df.iloc[:, 0].astype(bool)]
    data_df.rename({
        'index': 'label',
        0: 'value'
    }, axis=1, inplace=True)
    times = data_df.loc[[
        'created_at',
        'updated_at',
        'retrieved_at',
    ]]

    data_df.loc[[
        'created_at',
        'updated_at',
        'retrieved_at',
    ]] = times.apply(lambda x: pd.to_datetime(x, format="%Y-%m-%d").dt.date)
    data_df.index = data_df.index.str.replace('\.', '_')

    return (data_df, filings_df, officers_df)

def write_excel(data_df, filings_df, officers_df, events_df=None, output_dir=''):
    data_folder_name = (
        data_df.loc['name']
        .str.replace(' ', '_', regex=False)
        .str.replace('\.|,|\:|\;|\/|\$|\%|\@|\&', '', regex=True)
        .str.lower()
        [0]
    )
    data_file_name = data_folder_name
    out_path = os.path.join(output_dir, data_folder_name)
    dir_exists = os.path.isdir(out_path)
    if not dir_exists:
        os.makedirs(out_path)
    status = data_df.loc['status'][0]
    inactive_tag = ''
    if not is_active(status):
        inactive_tag = '--inactive--'
    excel_file_name = os.path.join(out_path, data_file_name) + inactive_tag +'.xlsx'
    file_exists = os.path.isfile(excel_file_name)

    with pd.ExcelWriter(excel_file_name, engine='xlsxwriter') as writer:
        if not file_exists:
            print(f'Writing "{data_file_name}.xlsx"...')
            data_df.to_excel(writer, sheet_name='company_data')
            officers_df.to_excel(writer, index=False,
                                    sheet_name='officers')
            filings_df.to_excel(writer, index=False, sheet_name='filings')
            if events_df is not None:
                events_df.to_excel(writer, index=False,
                                    sheet_name='events')
            if not is_active(status):
                workbook = writer.book
                worksheet = writer.sheets['company_data']
                cell_format = workbook.add_format({
                    'bold': True,
                    'font_color': 'red',
                    'bg_color': '#FF8E76'
                })
                worksheet.write(3,1, status, cell_format)

            
            print('Done!')

def scrape_company_url(url, output_dir=''):
    '''Takes a url to a single company and
        writes the gathered data to an excel spreadsheet. Can specify an output path 
        but will create a folder in the working directory otherwise.'''
    tree = req_to_lxml_tree(url)
    officers_df = process_officers(tree)
    data_df = process_main_page(tree)
    filings_df = process_filings(tree)
    events_df = process_events(tree)
    url = pd.DataFrame({'company_info':url}, index=["open_corp_url"])
    data_df = data_df.append(url)
    write_excel(data_df, filings_df, officers_df, events_df=events_df, output_dir=output_dir)


def generate_search_url(
    query_string, 
    state='', 
    officers=False, 
    position='', 
    ):
    """Takes in a plain query string of a company or officer and will return the url for all related results from this query.
    
    ---------
    
    query_string => string : A plain string from a spreadsheet or document to search for.

    state => string: returns non-state-filtered results by default, enter 'us_mo' or 'us_ks' for Missouri or Kansas.

    officers => Bool : if True will treat the query as an officer search else defaults to company search.

    position: Enter "agent", "organizer", "director" etc. to filter by officer position.
     """


    base_url = "https://opencorporates.com/"

    if officers:
        search_type = "officers?"
    else:
        search_type = "companies?"
    
    state = f"jurisdiction_code={state}&"
    
    if officers and position:
        position = f"position={position}&"

    query_string = re.sub('[,\.\/\-\'\"\?\!]', '', query_string).replace(' ', '+')
    query =  f"q={query_string}&"

    final_url = f"{base_url}{search_type}{state}{position}{query}utf8=%E2%9C%93"

    return final_url

def recursive_agent_search(url, state='', position='', fuzzy_match=''):
    '''Takes a company URL and will find all secondary related companies that the officers of the primary company are affiliated with
    
    --------------
    
    url  -> string : company url to start with.
    state -> string : to limit the search to certain jurisdictions, None is default, enter "us_mo" or "us_ks" for Missouri or Kansas.
    position -> string : filter by officer position, "agent", "organizer", "director" etc. to
    fuzzy_match -> string : Whether to return all results from the search or only ones that meet a match threshold with a given query. 
    Useful for combining with dp.generate_search_url() while searching from a list of LLC names 'blind'.
 '''
    #go to a company url 
    tree = req_to_lxml_tree(url)
    #find the names of the officers attached
    officer_names = tree.xpath('//a[contains(@class, "officer")]/text()')
    officers_dict = {}
    for name in officer_names:
        # generate a search url from the names of the officers
        agent_url = generate_search_url(name, officers=True, state=state, position=position)
        #go to the agent search results and get the urls of attached companies
        result_urls = get_urls(agent_url, fuzzy_match=fuzzy_match)
        officers_dict[name] = result_urls
    return officers_dict



    
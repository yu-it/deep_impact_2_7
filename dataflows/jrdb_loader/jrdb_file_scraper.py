# -*-coding:utf-8-*-
import dataflows.util as util
import re
import urllib2
from html.parser import HTMLParser


url_pattern = "http://www.jrdb.com/member/datazip/{table}/{path}"
class JrdbParser(HTMLParser):
    def __init__(self, table_name):
        HTMLParser.__init__(self)
        self.table_name = table_name
        self.list_of_links = []
        self.list_of_year_packs = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() <> "a":
            return

        for attr in attrs:
            if attr[0].lower() == "href" and self.table_name in attr[1].lower():
                date_string = file_name2date_string(attr[1])
                if len(date_string) == 4:
                    self.list_of_year_packs.append((date_string, "http://www.jrdb.com/member/datazip/{table}/{path}".format(table=self.table_name.capitalize(), path=attr[1])))
                else:
                    self.list_of_links.append((date_string, "http://www.jrdb.com/member/datazip/{table}/{path}".format(table=self.table_name.capitalize(), path=attr[1])))



def http_get_from_jrdb(url,http_client):
    print "req start:{url}".format(url = url)
    with open("C:\github\deep_impact_2_7\dataflows\jrdb_loader\#local\\auth_info.txt","r") as reader:
        user = reader.readline()[0:8]
        password = reader.readline()[0:8]
    headers = {}
    headers["authorization"] = "Basic " + (user + ":" + password).encode("base64")[:-1]
    req = urllib2.Request(url=url, headers=headers)
    res = urllib2.urlopen(req).read()
    print "end"

    return res

def cleansing_table_name(table_name):
    if re.match("a_.+",table_name):
        table_name = table_name.replace("a_","")
    return table_name

def get_all_zipfile_links(table_name):
    table_name = cleansing_table_name(table_name)
    parser = JrdbParser(table_name)
    parser.feed(http_get_from_jrdb(url_pattern.format(table= table_name.capitalize(),path="index.html"),None))
    return parser.list_of_year_packs, parser.list_of_links

def get_zipfile_links(table_name, from_date, to_date):
    list_of_year_packs, list_of_links = get_all_zipfile_links(table_name)
    from_year = from_date[0:4]
    to_year = to_date[0:4]
    ret = []
    year_packs = []

    #年度パックから抽出
    for year,url in list_of_year_packs:
        year_packs.append(year)
        if from_year <= year and year <= to_year:
            ret.append((year,url))

    #週次パックから抽出
    for date,url in list_of_links:
        if date[0:4] in year_packs:
            continue
        if from_date <= date and date <= to_date:
            ret.append({"args": (table_name,from_date,to_date),"data":(date,url)})
    return ret

def file_name2date_string(file_name):
    if "/" in file_name:
        file_name = re.sub(".+/","",file_name)
    if re.match(r"[a-z]{3}_\d{4}\.(txt|csv|zip)", file_name.lower()):
        return file_name[4:4 + 4]
    date_string = file_name[3:3 + 6]
    return  (("19" if date_string[0:1] == "9" else "20") + date_string)



def expand_zip2bytearray(table_name,entry, from_date, to_date):
    table_name = cleansing_table_name(table_name)
    ret = []
    entry_date, url = entry
    for file_name, byte_seq in util.extract_zip_file_entry(bytearray(http_get_from_jrdb(url,None))):
        date = file_name2date_string(file_name)
        if from_date <= date and date <= to_date and table_name in file_name.lower():
            ret.append((file_name,byte_seq))
    return ret

def get_jrdb_data(table_name, from_date, to_date):
    list = get_zipfile_links(table_name, from_date, to_date)
    data_array = []
    for entry in list:
        data_array.expand(expand_zip2bytearray(table_name, entry, from_date, to_date))


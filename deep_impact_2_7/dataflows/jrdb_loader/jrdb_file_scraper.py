# -*-coding:utf-8-*-

import deep_impact_2_7.util as util
import re
import urllib2
from html.parser import HTMLParser
import datetime
import collections
url_pattern = "http://www.jrdb.com/member/datazip/{table}/{path}"
url_master_index = "http://www.jrdb.com/member/data/"
url_master_2 = "http://www.jrdb.com/member/datazip/Ks/2018/KZA180908.zip"
class scraping_request:
    def __init__(self,a0,a1,a2):
        self.table_name=a0
        self.from_date = a1
        self.to_date=a2


zip_file_data = collections.namedtuple("zip_file_data", [
                        "file_name",
                        "byte_seq"
])
jrdb_auth_info = collections.namedtuple("jrdb_auth_info", [
                        "user_name",
                        "password"
                    ])

zipfile_link_info = collections.namedtuple("zipfile_link_info", [
                        "table_name",
                        "from_date",
                        "to_date",
                        "url"
                    ])
class JrdbParser(HTMLParser):
    def __init__(self, table_name):
        HTMLParser.__init__(self)
        self.table_name = table_name
        self.list_of_links = []
        self.list_of_year_packs = []
        self.rule_2_enable = False

    def handle_starttag(self, tag, attrs):
        if tag.lower() <> "a":
            return

        for attr in attrs:
            if attr[0].lower() == "href" and self.table_name in attr[1].lower() and ".zip" == attr[1][-4:].lower():
                date_string = file_name2date_string(attr[1])
                if len(date_string) == 4:
                    self.list_of_year_packs.append((date_string, "http://www.jrdb.com/member/datazip/{table}/{path}".format(table=self.table_name.capitalize(), path=attr[1])))
                else:
                    if self.rule_2_enable:
                        self.list_of_links.append((date_string,
                                                   "http://www.jrdb.com/member/datazip/{path}".format(
                                                       path=attr[1])))

                    else:
                        self.list_of_links.append((date_string,
                                                   "http://www.jrdb.com/member/datazip/{table}/{path}".format(
                                                       table=self.table_name.capitalize(), path=attr[1])))


user =""
password=""
def http_get_from_jrdb(url,http_client):
    util.debug("req start:{url}".format(url = url))
    #with open("C:\github\deep_impact_2_7\dataflows\jrdb_loader\#local\\auth_info.txt","r") as reader:
    #    user = reader.readline()[0:8]
    #    password = reader.readline()[0:8]
    user = http_client.user_name
    password = http_client.password
    headers = {}
    headers["authorization"] = "Basic " + (user + ":" + password).encode("base64")[:-1]
    req = urllib2.Request(url=url, headers=headers)
    res = urllib2.urlopen(req).read()
    util.debug("end")

    return res

def cleansing_table_name(table_name):
    if re.match("a_.+",table_name):
        table_name = table_name.replace("a_","")
    return table_name

def get_all_zipfile_links(table_name,auth_data):
    table_name = cleansing_table_name(table_name)
    parser = JrdbParser(table_name)
    try:
        parser.feed(http_get_from_jrdb(url_pattern.format(table= table_name.capitalize(),path="index.html"),auth_data))
    except urllib2.HTTPError as ex:
        if ex.code == 404:
            parser.rule_2_enable = True
            parser.feed(http_get_from_jrdb(url_master_index,auth_data))
        else:
            raise ex
    return parser.list_of_year_packs, parser.list_of_links

#transform (table,from, to) to (table, from, to, url
def get_zipfile_links(table_name, from_date, to_date, auth_data):
    list_of_year_packs, list_of_links = get_all_zipfile_links(table_name,auth_data)
    from_year = from_date[0:4]
    to_year = to_date[0:4]
    ret = []
    year_packs = []

    #年度パックから抽出
    for year,url in list_of_year_packs:
        year_packs.append(year)
        if from_year <= year and year <= to_year:

            ret.append(zipfile_link_info(table_name, from_date, to_date ,url))

    #週次パックから抽出
    for date,url in list_of_links:
        if date[0:4] in year_packs:
            continue
        if from_date <= date and date <= to_date:
            ret.append(zipfile_link_info(table_name, from_date, to_date ,url))
    return ret

def file_name2date_string(file_name):
    if "/" in file_name:
        file_name = re.sub(".+/","",file_name)
    if re.match(r"[a-z]{3}_\d{4}\.(txt|csv|zip)", file_name.lower()):
        return file_name[4:4 + 4]
    date_string = file_name[3:3 + 6]
    return  (("19" if date_string[0:1] == "9" else "20") + date_string)

#transform (table_name, from_date, to_date, url) to (date, byte_seq)
def download_zip_file_and_extract_data_needed(table_name, from_date, to_date, url, auth_info):
    table_name = cleansing_table_name(table_name)
    ret = []

    for file_name, byte_seq in util.extract_zip_file_entry(bytearray(http_get_from_jrdb(url, auth_info))):
        date = file_name2date_string(file_name)
        if date == "19999999":
            date = "19991231"
        if from_date <= date and date <= to_date and table_name in file_name.lower():
            try:
                ret.append(zip_file_data(
                    datetime.datetime.strptime(file_name2date_string(file_name), '%Y%m%d').strftime('%Y-%m-%d'),
                    byte_seq))
            except:
                pass
    return ret

def get_jrdb_data(table_name, from_date, to_date,user_id,password):
    list = get_zipfile_links(table_name, from_date, to_date,user_id,password)
    data_array = []
    for entry in list:
        data_array.expand(download_zip_file_and_extract_data_needed(table_name, entry, from_date, to_date,user_id,password))


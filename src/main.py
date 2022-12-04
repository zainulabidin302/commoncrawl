import ujson as json
try:
    # Python2
    from urlparse import urlparse
except ImportError:
    # Python3
    from urllib.parse import urlparse
from pyspark.sql.types import StructType, StructField, StringType, LongType
from sparkcc import CCSparkJob
from bs4 import BeautifulSoup
import shutil
import re

def extract_generator(x):
    try:
        meta = x.findAll('meta')
        return list(filter(lambda x: x.get('name') == 'generator', meta))[0].get('content')
    except Exception as e:
        return None
    
email_tester = re.compile("[a-z0-9\.\-+_]+@[a-z0-9\.\-+_]+\.[a-z]+")
def extract_all_emails(x):
    try:
        return '|'.join(set(email_tester.findall(x)))
    except Exception as e:
        return None

with open('contact_keywords.txt', 'r') as f:
    contact_kw = f.read().splitlines()

def contact_filter_by_kw(x):
    for kw in contact_kw:
        flag = False
        try:
            flag = x.get('href').lower().find(kw) >=0
        except Exception as e:
            return e
        if flag: return True
        try:
            flag = x.text.lower().find(kw) >=0
        except Exception as e:
            return e
        if flag: return True

    return flag
def extract_ssl_or_not(x):
    try:
        return urlparse(x).scheme
    except Exception as e:
        return None
    
def extract_contact(x):
    try:
        aa = x.findAll('a')
        return '|'.join(set(map(lambda x: x.get('href'), filter(contact_filter_by_kw, aa))))
    except Exception as e:
        return None

def extract_country_lang(x):
    try:
        lang_country = x.find('html').get('lang')
        a = lang_country.split('-')
        if len(a) == 2:
            lang, country = a
        elif len(a) == 1:
            lang, country = a[0], None 
        else:
            lang, country = None, None    
        return lang, country
    except Exception as e:
        return None, None
    
class Job(CCSparkJob):
    output_schema = StructType([
        StructField('conent_type', StringType(), True),
        StructField('record_type', StringType(), True),
        StructField('uri', StringType(), True),
        StructField('is_secure', StringType(), True),
        StructField('language', StringType(), True),
        StructField('country', StringType(), True),
        StructField('generator', StringType(), True),
        StructField('emails', StringType(), True),
        StructField('contact', StringType(), True)
    ])
    
    def process_record(self, record):
        record_type = record.rec_type
        uri = record.rec_headers.get_header('WARC-Target-URI')

        if record_type == 'response' and record.http_headers.get_header('Content-Type').find('text/html') >= 0:
            try:
                content = str(
                    record\
                    .content_stream()\
                    .read()\
                    .decode('utf-8')
                )
                bs = BeautifulSoup(content, features="lxml")
                generator = extract_generator(bs)
                contact = extract_contact(bs)
                lang, country = extract_country_lang(bs)
                emails  = extract_all_emails(content)
                is_secure = extract_ssl_or_not(uri)
                yield ('text/html', record_type, uri, is_secure, lang, country, generator, emails, contact)
            except Exception as e:
                yield ('error', record_type, uri, None, None, None, None, None, None)
            
if __name__ == "__main__":
    job = Job()
    job.run()
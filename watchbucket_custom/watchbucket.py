#!/usr/bin/python
#-*- coding: utf-8 -*-

from logtools import logger
import config as conf
from s3tools import connect_s3
import urllib2
import httplib
import ssl
import qencode
import json

log = logger('bucketwatch', path=None, log_dir=None)

S3_DATA = dict(
  host=dict(
    host=conf.S3_HOST,
    scheme=conf.S3_SCHEME
  ),
  access_id=conf.S3_KEY,
  access_key=conf.S3_SECRET,
  bucket=conf.S3_BUCKET
)
bucket = None
try:
  log.error('connect to: %s' % conf.S3_BUCKET)
  bucket = connect_s3(S3_DATA)
except Exception as e:
  log.error(e)


def cheack_maintype(url):
  try:
    _header = {'User-agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64)'}
    request = urllib2.Request(url, None, _header)
    response = urllib2.urlopen(request, timeout=5)
    http_message = response.info()
    full = http_message.type
    main = http_message.maintype
    if main != 'video':
      raise Exception('Content Error')
  except urllib2.URLError, e:
    log.error(e)
  except urllib2.HTTPError, e:
    log.error(e)
  except httplib.InvalidURL, e:
    log.error(e)
  except httplib.HTTPException, e:
    log.error(e)
  except ssl.SSLError, e:
    log.error(e)
  except Exception, e:
    log.error(e)
  else:
    return True


def get_unprocessed_files():
  if not bucket:
    return
  if conf.INPUT_PATH:
    prefix = "%s/" % conf.INPUT_PATH
    bucked_list = bucket.list(prefix=prefix)
  else:
    bucked_list = bucket.list(delimiter='/')
  encode_data = []
  for object_key in bucked_list:
    key = object_key
    key_name = object_key.key
    key_name_list = key_name.split('/')
    file_mane = key_name_list[len(key_name_list) - 1]
    if file_mane:
      key.set_acl('public-read')
      old_url = key.generate_url(2592000, query_auth=False)
      if not cheack_maintype(old_url):
        continue
      new_key_name = "%s/%s" % (conf.PROCESSING_PATH, file_mane)
      try:
        bucket.copy_key(new_key_name, conf.S3_BUCKET, key_name, preserve_acl=False)
      except Exception as e:
        log.error(e)
        continue
      key.delete()
      new_key = bucket.get_key(new_key_name)
      new_key.set_acl('public-read')
      url = new_key.generate_url(2592000, query_auth=False)
      if cheack_maintype(url):
        encode_data.append(dict(url=url, key_name=new_key.key))
  return encode_data

def mv_file(key_name):
  if not bucket:
    return
  key_name_list = key_name.split('/')
  file_mane = key_name_list[len(key_name_list) - 1]
  new_key_name = "%s/%s" % (conf.PROCESSED_PATH, file_mane)
  try:
    bucket.copy_key(new_key_name, conf.S3_BUCKET, key_name, preserve_acl=False)
  except Exception as e:
    log.error(e)
    return
  old_key = bucket.get_key(key_name)
  old_key.delete()

def status_callback(status, key, task_token):
  log.error("%s: %s" % (task_token, repr(status)))
  if status:
    if status['error'] == 0 and status['status'] == 'completed':
      mv_file(key)

def get_query_template():
  try:
    with open(conf.QUERY_FILE_PATH) as data:
      try:
        file_data = json.load(data)
        return file_data
        # return json.dumps(file_data)
      except ValueError as e:
        log.error(e)
  except IOError as e:
    log.error(e)

def prepare_query(query_template, **kwargs):
  query = query_template.get('query')
  query['source'] = kwargs.get('source_url') if kwargs.get('source_url') else ""
  return json.dumps(query_template)



def worker():
  data = get_unprocessed_files()
  log.error('got files: %s' % repr(data))
  if not data:
    return
  client = qencode.client(conf.QENCODE_API_KEY)
  if client.error:
    log.error(client.message)
    raise
  query_template = get_query_template()
  for item in data:
    task = client.create_task()
    if task.error:
      log.error(task.message)
      continue
    query = prepare_query(query_template, source_url=item['url'])
    task.custom_start(None, query=query)
    if task.error:
      log.error(task.message)
      continue
    task.progress_changed(status_callback, item.get('key_name'), task.task_token)



if __name__ == "__main__":
  try:
    worker()
  except Exception as e:
    log.error(e)

#!/usr/bin/python
#-*- coding: utf-8 -*-

try:
  import boto
except ImportError:
  pass
else:
  from boto.s3.connection import OrdinaryCallingFormat


def _connect(access_id, access_key, host=None, port=None, is_secure=False):
  return boto.connect_s3(
    access_id, access_key, host=host, port=port, is_secure=is_secure,
    calling_format=OrdinaryCallingFormat()
  )

def connect_s3(s3_data):
  host = s3_data.get('host')
  if host and host['host'] and host['scheme']:
    is_secure = host['scheme'] == 'https'
    s3 = _connect(
      s3_data['access_id'], s3_data['access_key'],
      host=host['host'], port=host.get('port'), is_secure=is_secure
    )
    bucket = s3.get_bucket(s3_data['bucket'])
  else:
    bucket = None
  return bucket

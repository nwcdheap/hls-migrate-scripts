#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import _3_compare_ts

if __name__ == '__main__':
    maxThreads = 800
    bucket = ''
    prefix = '/'   #最前面不要/,最后要/，比如 abc/123/
    
    _3_compare_ts.upload_again(bucket, prefix, maxThreads)


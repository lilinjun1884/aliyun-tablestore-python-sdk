# -*- coding: utf8 -*-

import base64
import hashlib
import hmac
from abc import ABC, abstractmethod
from functools import lru_cache

import six

try:
    from urlparse import urlparse, parse_qsl
    from urllib import urlencode
except ImportError:
    from urllib.parse import urlparse, parse_qsl, urlencode

import tablestore.consts as consts
import tablestore.utils as utils
from tablestore.credentials import Credentials
from tablestore.error import *


def calculate_hmac(signing_key, signature_string, sign_method, encoding):
    if isinstance(signing_key, six.text_type):
        signing_key = signing_key.encode(encoding)
    if isinstance(signature_string, six.text_type):
        signature_string = signature_string.encode(encoding)
    return hmac.new(signing_key, signature_string, sign_method).digest()


def call_signature_method_sha1(signing_key, signature_string, encoding):
    # The signature method is supposed to be HmacSHA1
    return base64.b64encode(calculate_hmac(signing_key, signature_string, hashlib.sha1, encoding)).decode(encoding)


def call_signature_method_sha256(signing_key, signature_string, encoding):
    # The signature method is supposed to be HmacSHA256
    return base64.b64encode(calculate_hmac(signing_key, signature_string, hashlib.sha256, encoding)).decode(encoding)


class RequestContext(object):
    def __init__(self, credentials: Credentials, sign_date: str = None):
        self.credentials = credentials
        self.sign_date = sign_date

    def get_credentials(self) -> Credentials:
        return self.credentials

    def set_sign_date(self, sign_date: str):
        self.sign_date = sign_date

    def get_sign_date(self) -> str:
        return self.sign_date

class SignBase(ABC):
    def __init__(self, encoding: str, **kwargs):
        self.encoding = encoding

    @staticmethod
    def _make_headers_string(headers):
        headers_item = ["%s:%s" % (k.lower(), v.strip()) for k, v in headers.items()
                        if k.startswith(consts.OTS_HEADER_PREFIX)]
        return "\n".join(sorted(headers_item))

    def _get_request_signature_string(self, query, headers):
        uri, param_string, query_string = urlparse(query)[2:5]

        # TODO a special query should be input to test query sorting,
        # because none of the current APIs uses query map, but the sorting
        # is required in the protocol document.
        query_pairs = parse_qsl(query_string)
        sorted_query = urlencode(sorted(query_pairs))
        signature_string = uri + '\n' + 'POST' + '\n' + sorted_query + '\n'

        headers_string = self._make_headers_string(headers)
        signature_string += headers_string + '\n'
        return signature_string

    def make_response_signature(self, query, headers, signing_key):
        uri = urlparse(query)[2]
        headers_string = self._make_headers_string(headers)
        signature_string = headers_string + '\n' + uri
        # Response signature use same signing key as request signature
        # But the signature method is supposed to be HmacSHA1
        signature = call_signature_method_sha1(signing_key, signature_string, self.encoding)
        return signature

    @abstractmethod
    def get_signing_key(self, request_context: RequestContext):
        pass

    @abstractmethod
    def make_request_signature_and_add_headers(self, query, headers, request_context: RequestContext):
        pass


class SignV2(SignBase):
    def __init__(self, encoding: str, **kwargs):
        SignBase.__init__(self, encoding, **kwargs)

    def get_signing_key(self, request_context: RequestContext):
        return request_context.get_credentials().get_access_key_secret()

    def make_request_signature_and_add_headers(self, query, headers, request_context: RequestContext):
        signature_string = self._get_request_signature_string(query, headers)
        signing_key = self.get_signing_key(request_context)
        headers[consts.OTS_HEADER_SIGNATURE] = call_signature_method_sha1(
            signing_key,
            signature_string,
            self.encoding
        )

class SignV4(SignBase):
    def __init__(self, encoding: str, **kwargs):
        SignBase.__init__(self, encoding, **kwargs)

        self.sign_region = kwargs.get('region')
        if not isinstance(self.sign_region, str) or self.sign_region == '':
            raise OTSClientError('region is not str or is empty.')

        self.sign_date = kwargs.get('sign_date')
        self.auto_update_v4_sign = (kwargs.get('auto_update_v4_sign') is True)
        if self.sign_date is None:
            self.sign_date = utils.get_now_utc_datetime().strftime(consts.V4_SIGNATURE_SIGN_DATE_FORMAT)
            self.auto_update_v4_sign = True

    @staticmethod
    @lru_cache(maxsize=128)
    def _get_v4_signing_key(access_key_secret: str, sign_date: str, sign_region: str, encoding: str) -> bytes:
        origin_signing_key = consts.V4_SIGNATURE_PREFIX + access_key_secret
        first_signing_key = calculate_hmac(origin_signing_key, sign_date, hashlib.sha256, encoding)
        second_signing_key = calculate_hmac(first_signing_key, sign_region, hashlib.sha256, encoding)
        third_signing_key = calculate_hmac(second_signing_key, consts.V4_SIGNATURE_PRODUCT, hashlib.sha256, encoding)
        fourth_signing_key = calculate_hmac(third_signing_key, consts.V4_SIGNATURE_CONSTANT, hashlib.sha256, encoding)
        return base64.b64encode(fourth_signing_key)

    def _get_and_set_v4_sign_date(self, request_context: RequestContext) -> str:
        if self.auto_update_v4_sign:
            cur_date = utils.get_now_utc_datetime().strftime(consts.V4_SIGNATURE_SIGN_DATE_FORMAT)
        else:
            cur_date = self.sign_date
        if cur_date is None:
            raise OTSClientError('v4 sign_date is None.')

        request_context.set_sign_date(cur_date)
        return cur_date

    def get_signing_key(self, request_context: RequestContext):
        cur_key_secret = request_context.get_credentials().get_access_key_secret()
        cur_date = request_context.get_sign_date()
        cur_region = self.sign_region

        return self._get_v4_signing_key(cur_key_secret, cur_date, cur_region, self.encoding)

    def make_request_signature_and_add_headers(self, query, headers, request_context: RequestContext):
        headers[consts.OTS_HEADER_SIGN_DATE] = self._get_and_set_v4_sign_date(request_context)
        headers[consts.OTS_HEADER_SIGN_REGION] = self.sign_region
        signature_string = self._get_request_signature_string(query, headers)
        signature_string += consts.V4_SIGNATURE_SALT
        signing_key = self.get_signing_key(request_context)
        headers[consts.OTS_HEADER_SIGNATURE_V4] = call_signature_method_sha256(
            signing_key,
            signature_string,
            self.encoding
        )

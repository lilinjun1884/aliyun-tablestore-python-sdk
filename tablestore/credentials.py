from abc import ABC, abstractmethod


class Credentials(object):
    def __init__(self, access_key_id: str, access_key_secret: str, security_token: str = None):
        self._access_key_id = access_key_id
        self._access_key_secret = access_key_secret
        self._security_token = security_token

    def get_access_key_id(self) -> str:
        return self._access_key_id

    def get_access_key_secret(self) -> str:
        return self._access_key_secret

    def get_security_token(self) -> str:
        return self._security_token


class CredentialsProvider(ABC):
    @abstractmethod
    def get_credentials(self) -> Credentials:
        pass


class StaticCredentialsProvider(CredentialsProvider):
    def __init__(self, access_key_id: str, access_key_secret: str = None, security_token: str = None):
        self._credentials = Credentials(access_key_id, access_key_secret, security_token)

    def get_credentials(self) -> Credentials:
        return self._credentials

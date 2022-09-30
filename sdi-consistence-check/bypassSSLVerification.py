import requests
from requests import Session


def bypassSSLVerification():
    old_request_method = requests.request

    def new_request_method(method, url, headers=None, **kw):
        return old_request_method(method, url, headers=headers, verify=False, **kw)

    requests.request = new_request_method

    old_post_method = requests.post

    def new_post_method(url, request, headers=None, **rkwargs):
        return old_post_method(url, request, headers=headers, verify=False, **rkwargs)
    requests.post = new_post_method

    requests.packages.urllib3.disable_warnings()

    old_get_method = Session.get

    def new_get_method(self, url, **kwargs):
        return old_get_method(self, url, verify=False, **kwargs)

    Session.get = new_get_method


if __name__ == "__main__":
    bypassSSLVerification()

    requests.request("GET", "https://docs.python-requests.org/")
    requests.post("https://docs.python-requests.org/", None)

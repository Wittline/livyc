import requests

class JsonClient:
    def __init__(self, url: str) -> None:
        self.url = url
        self.session = requests.Session()
        self.session.headers.update({'X-Requested-By': 'ambari'})

    def close(self) -> None:
        self.session.close()

    def get(self, endpoint: str = '') -> dict:
        return self._request('GET', endpoint)

    def post(self, endpoint: str, data: dict = None) -> dict:
        return self._request('POST', endpoint, data)

    def delete(self, endpoint: str = '') -> dict:
        return self._request('DELETE', endpoint)

    def _request(self, method: str, endpoint: str, data: dict = None) -> dict:
        url = self.url.rstrip('/') + endpoint        
        response = self.session.request(method, url, json=data)
        response.raise_for_status()
        return response.json()
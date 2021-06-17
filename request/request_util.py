import asyncio
import contextvars
import functools
import traceback
import typing
from asyncio import iscoroutinefunction

import codecs
from collections import deque

import cchardet
import ujson
from aiohttp import helpers
from loguru import logger
from parsel import Selector
import aiohttp
import requests
from aiohttp import TCPConnector

T = typing.TypeVar("T")


async def run_in_threadpool(
    func: typing.Callable[..., T], *args: typing.Any, **kwargs: typing.Any
) -> T:
    loop = asyncio.get_event_loop()
    if contextvars is not None:  # pragma: no cover
        # Ensure we run in the same context
        child = functools.partial(func, *args, **kwargs)
        context = contextvars.copy_context()
        func = context.run
        args = (child,)
    elif kwargs:  # pragma: no cover
        # loop.run_in_executor doesn't accept 'kwargs', so bind them in here
        func = functools.partial(func, **kwargs)
    return await loop.run_in_executor(None, func, *args)


async def run_function(callable_fun,  *args, **kwargs) -> typing.Any:
    if iscoroutinefunction(callable_fun):
        return await callable_fun(*args, **kwargs)
    else:
        return await run_in_threadpool(callable_fun, *args, **kwargs)


def retry_fun():
    def retry(func):
        @functools.wraps(func)
        async def wrapper(self, *args, **kwargs):
            while True:
                response = await func(self, *args, **kwargs)
                # 成功返回response
                if response.ok == 1:
                    return response
        return wrapper
    return retry


class Request:
    method = None
    url = None

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def __repr__(self):
        return f"<{self.method} {self.url}>"


class Response:
    def __init__(
        self,
        url: str = "",
        *,
        encoding: str = "",
        content=None,
        cookies=None,
        history=None,
        headers=None,
        status: int = -1,
    ):
        self.url = url
        self.encoding = encoding
        self.headers = headers
        self.cookies = cookies
        self.history = history
        self.status = status
        self._content = content

    @property
    def content(self):
        return self._content

    @property
    def text(self):
        if not self._content:
            return self._content

        if not self.encoding:
            self.encoding = self.get_encoding()

        return self._content.decode(self.encoding, errors='replace')

    def json(self, *args, **kwargs):
        return ujson.loads(self.text, *args, **kwargs)

    @property
    def selector(self):
        return Selector(self.text)

    def xpath(self, xpath_str):
        return self.selector.xpath(xpath_str)

    def re(self, re_str):
        return self.selector.re(re_str)

    def css(self, css_str):
        return self.selector.css(css_str)

    def get_encoding(self) -> str:
        c_type = self.headers.get("Content-Type", "").lower()
        mimetype = helpers.parse_mimetype(c_type)

        encoding = mimetype.parameters.get("charset")
        if encoding:
            try:
                codecs.lookup(encoding)
            except LookupError:
                encoding = None
        if not encoding:
            if mimetype.type == "application" and (mimetype.subtype == "json" or mimetype.subtype == "rdap"):
                encoding = "utf-8"
            elif self._content is None:
                raise RuntimeError("Cannot guess the encoding of " "a not yet read body")
            else:
                encoding = cchardet.detect(self._content)["encoding"]
        if not encoding:
            encoding = "utf-8"

        return encoding

    def __repr__(self):
        return f"<Response [{self.status}]>"


class DownloaderMiddleware:
    middlewares: list = None

    def __init__(self):
        # request middleware
        self.request_middleware = deque()
        # response middleware
        self.response_middleware = deque()
        # close method
        self.close_method = deque()

    def _load_middleware(self, middlewares):
        for mw_cls in middlewares:
            mw = mw_cls()
            if hasattr(mw, "process_request") and callable(getattr(mw, "process_request")):
                self.request_middleware.append(mw.process_request)

            if hasattr(mw, "process_response") and callable(getattr(mw, "process_response")):
                self.response_middleware.appendleft(mw.process_response)

            if hasattr(mw, "close") and callable(getattr(mw, "close")):
                self.close_method.appendleft(mw.close)

    def _fetch(self, download_func, request):
        self.process_request(request, self)
        response = download_func(**request.__dict__)
        self.process_response(request, response, self)
        return response

    def process_request(self, request: Request, download_ins):
        for middleware in self.request_middleware:
            try:
                middleware(request, download_ins)
            except Exception as e:
                logger.error(f"<Middleware {middleware.__name__}: {e} \n{traceback.format_exc()}>")

    def process_response(self, request: Request, response: Response, download_ins):
        for middleware in self.response_middleware:
            try:
                middleware(request, response, download_ins)
            except Exception as e:
                logger.error(f"<Middleware {middleware.__name__}: {e} \n{traceback.format_exc()}>")

    def close(self):
        for func in self.close_method:
            func()


class Downloader(DownloaderMiddleware):
    default_session = None
    downloader_cls = None

    def __init__(self, middlewares):
        super().__init__()
        self._load_middleware(middlewares)

    def close(self):
        super().close()
        self.default_session.close()

    def get(self, *args, **kwargs):
        pass

    def post(self, *args, **kwargs):
        pass

    async def async_get(self, url, headers=None, cookies=None, params=None, allow_redirects=True, verify=None, **kwargs):
        return await run_function(self.get, url=url, headers=headers, cookies=cookies, params=params,
                                  allow_redirects=allow_redirects, verify=verify, **kwargs)

    async def async_post(self, url, headers=None, cookies=None, params=None, data=None, json=None, allow_redirects=True,
                         verify=None, **kwargs):
        return await run_function(self.post, url=url, headers=headers, cookies=cookies, params=params, data=data,
                                  json=json, allow_redirects=allow_redirects, verify=verify, **kwargs)


class RequestDownloader(Downloader):
    def __init__(self, session=None, middlewares=None, retry_times=3, retry_delay=3):
        super().__init__(middlewares)
        self.retry_times = retry_times
        self.retry_delay = retry_delay
        self.middlewares = middlewares
        self.downloader_cls = requests

        if session:
            self.default_session = session
        else:
            self.default_session = requests.Session()

    def get(self, url, headers=None, cookies=None, params=None, allow_redirects=True, verify=None, **kwargs):
        return requests.get(url=url, headers=headers, params=params, cookies=cookies, allow_redirects=allow_redirects,
                            verify=verify, **kwargs)

    def post(self, url, headers=None, cookies=None, allow_redirects=True, params=None, data=None, json=None,
             verify=None, **kwargs):
        return requests.post(url=url, headers=headers, cookies=cookies, allow_redirects=allow_redirects, params=params,
                             data=data, json=json, verify=verify, **kwargs)

    def download(self, url, method="GET", params=None, data=None, headers=None, cookies=None, files=None, auth=None,
                 timeout=None, allow_redirects=True, proxies=None, hooks=None, stream=None, verify=None, cert=None,
                 json=None, session=None):
        kwargs = locals().copy()
        kwargs.pop("self")
        kwargs.pop("session")
        if session:
            return session.request(**kwargs)
        else:
            return self.default_session.request(**kwargs)

    def fetch(self, url, method="GET", params=None, data=None, headers=None, cookies=None, files=None, auth=None,
              timeout=None, allow_redirects=True, proxies=None, hooks=None, stream=None, verify=None, cert=None,
              json=None, session=None):

        kwargs = locals().copy()
        kwargs.pop("self")
        return self._fetch(self.download, Request(**kwargs))

    async def async_download(self, url, method="GET", params=None, data=None, headers=None, cookies=None, files=None,
                             auth=None, timeout=None, allow_redirects=True, proxies=None, hooks=None, stream=None,
                             verify=None, cert=None, json=None, session=None):
        kwargs = locals().copy()
        kwargs.pop("self")
        return await run_function(self.download, **kwargs)

    async def async_fetch(self, url, method="GET", params=None, data=None, headers=None, cookies=None, files=None,
                          auth=None, timeout=None, allow_redirects=True, proxies=None, hooks=None, stream=None,
                          verify=None, cert=None, json=None, session=None):
        kwargs = locals().copy()
        kwargs.pop("self")
        return await run_function(self.fetch, **kwargs)


class TestMiddleware:
    def process_request(self, request, download_ins):
        print("Middleware", request)

    def process_response(self, request, response, download_ins):
        print("Middleware", response)


if __name__ == '__main__':
    downloader = RequestDownloader(middlewares=[TestMiddleware])
    result = downloader.get("https://httpbin.org/get")
    print(result)
    result = downloader.post("https://httpbin.org/post")
    print(result)
    result = downloader.download("https://httpbin.org/get")
    print(result)
    result = downloader.download("https://httpbin.org/post", method="POST")
    print(result)
    result = downloader.fetch("https://httpbin.org/get")
    print(result)
    result = downloader.fetch("https://httpbin.org/post", method="POST")
    print(result)
    result = asyncio.run(downloader.async_get("https://httpbin.org/get"))
    print(result)
    result = asyncio.run(downloader.async_post("https://httpbin.org/post"))
    print(result)
    result = asyncio.run(downloader.async_download("https://httpbin.org/get"))
    print(result)
    result = asyncio.run(downloader.async_download("https://httpbin.org/post", method="POST"))
    print(result)
    result = asyncio.run(downloader.async_fetch("https://httpbin.org/get"))
    print(result)
    result = asyncio.run(downloader.async_fetch("https://httpbin.org/post", method="POST"))
    print(result)


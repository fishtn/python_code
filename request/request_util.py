import asyncio
import contextvars
import functools
import inspect
import time
import traceback
import typing
from asyncio import iscoroutinefunction

import codecs
from collections import deque

import cchardet
import httpx
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


def retry():
    def retry_fun(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            download_ins = args[0]
            while True:
                try:
                    response = func(*args, **kwargs)
                    if response.ok:
                        return response
                except:
                    logger.error(traceback.format_exc())

                retry_times = kwargs.get("retry_times", download_ins.retry_times)
                retry_delay = kwargs.get("retry_delay", download_ins.retry_delay)

                if retry_times <= 0:
                    raise Exception("请求错误，重试次数为零")

                kwargs["retry_times"] = retry_times - 1

                time.sleep(retry_delay)

        return wrapper
    return retry_fun


class Request:
    method = None
    url = None

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def __repr__(self):
        return f"<{self.method} {self.url}>"

    def get_http_kwargs(self):
        _kwargs = self.__dict__.copy()
        _kwargs.pop("retry_times", None)
        _kwargs.pop("retry_delay", None)
        return _kwargs


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
        self.ok = 1 if self.status < 400 else 0

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
        if not middlewares:
            return
        for mw_cls in middlewares:
            mw = mw_cls()
            if hasattr(mw, "process_request") and callable(getattr(mw, "process_request")):
                self.request_middleware.append(mw.process_request)

            if hasattr(mw, "process_response") and callable(getattr(mw, "process_response")):
                self.response_middleware.appendleft(mw.process_response)

            if hasattr(mw, "close") and callable(getattr(mw, "close")):
                self.close_method.appendleft(mw.close)

    def download_with_middleware(self, download_func, request):
        self.process_request(request, self)
        kwargs = request.get_http_kwargs()
        response = download_func(**kwargs)
        self.process_response(request, response, self)
        return response

    async def async_download_with_middleware(self, download_func, request):
        await run_function(self.process_request, request, self)
        kwargs = request.get_http_kwargs()
        response = await download_func(**kwargs)
        await run_function(self.process_response, request, response, self)
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

    def __init__(self, middlewares, retry_times, retry_delay):
        super().__init__()
        self.retry_times = retry_times
        self.retry_delay = retry_delay
        self._load_middleware(middlewares)

    def __del__(self):
        self.close()

    def close(self):
        super().close()

    def _request(self, func, **kwargs):
        resp = func(**kwargs)
        return Response(
            url=str(resp.url),
            content=resp.content,
            status=resp.status_code,
            cookies=resp.cookies,
            headers=resp.headers,
            history=resp.history
        )

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
        super().__init__(middlewares, retry_times, retry_delay)
        self.downloader_cls = requests

        if session:
            self.default_session = session
        else:
            self.default_session = requests.Session()
            self.default_session.trust_env = False

    @retry()
    def get(self, url, headers=None, cookies=None, params=None, allow_redirects=True, verify=None, **kwargs):
        _kwargs = locals().copy()
        _kwargs.pop("self")
        _kwargs.pop("kwargs")
        return self._request(requests.get, **_kwargs, **kwargs)

    @retry()
    def post(self, url, headers=None, cookies=None, allow_redirects=True, params=None, data=None, json=None,
             verify=None, **kwargs):
        _kwargs = locals().copy()
        _kwargs.pop("self")
        _kwargs.pop("kwargs")
        return self._request(requests.post, **_kwargs, **kwargs)

    @retry()
    def download(self, url, method="GET", params=None, data=None, headers=None, cookies=None, files=None, auth=None,
                 timeout=None, allow_redirects=True, proxies=None, hooks=None, stream=None, verify=None, cert=None,
                 json=None, session=None, **kwargs):
        _kwargs = locals().copy()
        _kwargs.pop("self")
        _kwargs.pop("session")
        _kwargs.pop("kwargs")
        if not session:
            session = self.default_session
        return self._request(session.request, **_kwargs, **kwargs)

    @retry()
    def fetch(self, url, method="GET", params=None, data=None, headers=None, cookies=None, files=None, auth=None,
              timeout=None, allow_redirects=True, proxies=None, hooks=None, stream=None, verify=None, cert=None,
              json=None, session=None, **kwargs):

        _kwargs = locals().copy()
        _kwargs.pop("self")
        _kwargs.pop("session")
        _kwargs.pop("kwargs")
        if not session:
            session = self.default_session
        return self.download_with_middleware(self._request, Request(func=session.request, **_kwargs, **kwargs))

    async def async_download(self, url, method="GET", params=None, data=None, headers=None, cookies=None, files=None,
                             auth=None, timeout=None, allow_redirects=True, proxies=None, hooks=None, stream=None,
                             verify=None, cert=None, json=None, session=None, **kwargs):
        _kwargs = locals().copy()
        _kwargs.pop("self")
        _kwargs.pop("kwargs")
        return await run_function(self.download, **_kwargs, **kwargs)

    async def async_fetch(self, url, method="GET", params=None, data=None, headers=None, cookies=None, files=None,
                          auth=None, timeout=None, allow_redirects=True, proxies=None, hooks=None, stream=None,
                          verify=None, cert=None, json=None, session=None, **kwargs):
        _kwargs = locals().copy()
        _kwargs.pop("self")
        _kwargs.pop("kwargs")
        return await run_function(self.fetch, **_kwargs, **kwargs)


class HttpxDownloader(Downloader):
    def __init__(self, session=None, middlewares=None, retry_times=3, retry_delay=3):
        super().__init__(middlewares, retry_times, retry_delay)
        self.downloader_cls = httpx

        if session:
            self.default_session = session
        else:
            self.default_session = httpx.Client(verify=False, http2=False, trust_env=False)

    @retry()
    def get(self, url, headers=None, cookies=None, params=None, allow_redirects=True, timeout=None, auth=None,
            trust_env=False, **kwargs):
        _kwargs = locals().copy()
        _kwargs.pop("self")
        _kwargs.pop("kwargs")
        return self._request(httpx.get, **_kwargs, **kwargs)

    @retry()
    def post(self, url, headers=None, cookies=None, allow_redirects=True, content=None, params=None, data=None,
             files=None, json=None, auth=None, timeout=None, trust_env=False, **kwargs):
        _kwargs = locals().copy()
        _kwargs.pop("self")
        _kwargs.pop("kwargs")
        return self._request(httpx.post, **_kwargs, **kwargs)

    @retry()
    def download(self, url, method="GET", params=None, data=None, headers=None, cookies=None, files=None, auth=None,
                 timeout=None, allow_redirects=True, content=None, proxies=None, json=None, session=None, **kwargs):
        _kwargs = locals().copy()
        _kwargs.pop("self")
        _kwargs.pop("session")
        _kwargs.pop("kwargs")
        _kwargs.pop("proxies")

        if proxies:
            if session:
                session = session.copy()
                session.proxies = proxies
            else:
                session = httpx.Client(verify=False, http2=False, proxies=proxies)

        if not session:
            session = self.default_session

        return self._request(session.request, **_kwargs, **kwargs)

    @retry()
    def fetch(self, url, method="GET", params=None, data=None, headers=None, cookies=None, files=None, auth=None,
              timeout=None, allow_redirects=True, content=None, proxies=None, json=None, session=None, **kwargs):
        _kwargs = locals().copy()
        _kwargs.pop("self")
        _kwargs.pop("session")
        _kwargs.pop("kwargs")
        _kwargs.pop("proxies")
        if proxies:
            if session:
                session = session.copy()
                session.proxies = proxies
            else:
                session = httpx.Client(verify=False, http2=False, proxies=proxies)

        if not session:
            session = self.default_session
        return self.download_with_middleware(self._request, Request(func=session.request, **_kwargs, **kwargs))

    async def async_download(self, url, method="GET", params=None, data=None, headers=None, cookies=None, files=None,
                             auth=None, timeout=None, allow_redirects=True, content=None, proxies=None, json=None,
                             session=None, **kwargs):
        _kwargs = locals().copy()
        _kwargs.pop("self")
        _kwargs.pop("kwargs")

        return await run_function(self.download, **_kwargs, **kwargs)

    async def async_fetch(self, url, method="GET", params=None, data=None, headers=None, cookies=None, files=None,
                          auth=None, timeout=None, allow_redirects=True, content=None, proxies=None, json=None,
                          session=None, **kwargs):
        _kwargs = locals().copy()
        _kwargs.pop("self")
        _kwargs.pop("kwargs")
        return await run_function(self.fetch, **_kwargs, **kwargs)


class AiohttpDownloader(Downloader):
    def __init__(self, session=None, middlewares=None, retry_times=3, retry_delay=3, loop=None):
        super().__init__(middlewares, retry_times, retry_delay)
        self.downloader_cls = aiohttp
        self.tc = None
        self.loop = loop if loop else asyncio.get_event_loop()
        if session:
            self.default_session = session

    def run_in_async(self, func, **kwargs):
        return self.loop.run_until_complete(func(**kwargs))

    async def get_session(self):
        if self.default_session:
            return self.default_session
        else:
            jar = aiohttp.DummyCookieJar()
            self.tc = TCPConnector(limit=0, force_close=True, enable_cleanup_closed=True, ssl=False)
            self.default_session = aiohttp.ClientSession(connector=self.tc, cookie_jar=jar)
            return self.default_session

    def close(self):
        super(AiohttpDownloader, self).close()
        if self.default_session:
            self.run_in_async(self.default_session.close)
            self.tc.close()

    async def _request(self, func, **kwargs):
        resp = await func(**kwargs)
        return Response(
            url=str(resp.url),
            content=await resp.read(),
            status=resp.status,
            cookies=resp.cookies,
            headers=resp.headers,
            history=resp.history,
        )

    async def async_get(self, url, headers=None, cookies=None, params=None, allow_redirects=True, ssl=None, proxy=None,
                        **kwargs):
        async with aiohttp.ClientSession() as client:
            async with client.request(method='GET', url=url, headers=headers, cookies=cookies, params=params,
                                      allow_redirects=allow_redirects, proxy=proxy, ssl=ssl, **kwargs) as resp:
                response = Response(
                    url=str(resp.url),
                    content=await resp.read(),
                    status=resp.status,
                    cookies=resp.cookies,
                    headers=resp.headers,
                    history=resp.history,
                )
                return response

    async def async_post(self, url, headers=None, cookies=None, allow_redirects=True, params=None,
                         data=None, json=None, auth=None, timeout=None, ssl=None, proxy=None, **kwargs):
        async with aiohttp.ClientSession() as client:
            async with client.request(method='POST', url=url, headers=headers, cookies=cookies, ssl=ssl, proxy=proxy,
                                      auth=auth, allow_redirects=allow_redirects, params=params, data=data, json=json,
                                      **kwargs) as resp:
                response = Response(
                    url=str(resp.url),
                    content=await resp.read(),
                    status=resp.status,
                    cookies=resp.cookies,
                    headers=resp.headers,
                    history=resp.history,
                )
                return response

    @retry()
    def get(self, url, headers=None, cookies=None, params=None, allow_redirects=True, ssl=None, proxy=None, **kwargs):
        _kwargs = locals().copy()
        _kwargs.pop("self")
        _kwargs.pop("kwargs")
        return self.run_in_async(self.async_get, **_kwargs, **kwargs)

    @retry()
    def post(self, url, headers=None, cookies=None, allow_redirects=True, params=None, data=None,
             json=None, auth=None, timeout=None, ssl=None, proxy=None, **kwargs):
        _kwargs = locals().copy()
        _kwargs.pop("self")
        _kwargs.pop("kwargs")
        return self.run_in_async(self.async_post, **_kwargs, **kwargs)

    @retry()
    def download(self, url, method="GET", headers=None, cookies=None, allow_redirects=True, params=None, data=None,
                 json=None, auth=None, timeout=None, ssl=None, proxy=None, session=None, **kwargs):
        _kwargs = locals().copy()
        _kwargs.pop("self")
        _kwargs.pop("kwargs")
        return self.run_in_async(self.async_download, **_kwargs, **kwargs)

    @retry()
    def fetch(self, url, method="GET", headers=None, cookies=None, allow_redirects=True, params=None, data=None,
              json=None, auth=None, timeout=None, ssl=None, proxy=None, session=None, **kwargs):
        _kwargs = locals().copy()
        _kwargs.pop("self")
        _kwargs.pop("kwargs")
        return self.run_in_async(self.async_fetch, **_kwargs, **kwargs)

    async def async_download(self, url, method="GET", headers=None, cookies=None, allow_redirects=True, params=None,
                             data=None, json=None, auth=None, timeout=None, ssl=None, proxy=None, session=None, **kwargs):
        _kwargs = locals().copy()
        _kwargs.pop("self")
        _kwargs.pop("session")
        _kwargs.pop("kwargs")

        if not session:
            session = await self.get_session()

        return await self._request(session.request, **_kwargs, **kwargs)

    async def async_fetch(self, url, method="GET", headers=None, cookies=None, allow_redirects=True, params=None,
                          data=None, json=None, auth=None, timeout=None, ssl=None, proxy=None, session=None, **kwargs):
        _kwargs = locals().copy()
        _kwargs.pop("self")
        _kwargs.pop("session")
        _kwargs.pop("kwargs")

        if not session:
            session = await self.get_session()
        return await self.async_download_with_middleware(self._request, Request(func=session.request, **_kwargs, **kwargs))


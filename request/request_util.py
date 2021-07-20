import asyncio
import atexit
import contextvars
import functools
import sys
import time
import typing
from asyncio import iscoroutinefunction, events

import codecs

import cchardet
import httpx
import ujson
from aiohttp import helpers
from parsel import Selector
import aiohttp
import requests
from aiohttp import TCPConnector


if sys.platform.startswith('win') and sys.version_info >= (3, 8):
    policy = asyncio.WindowsSelectorEventLoopPolicy()
    asyncio.set_event_loop_policy(policy)

try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass


async def to_thread(func, *args, **kwargs):
    loop = events.get_running_loop()
    ctx = contextvars.copy_context()
    func_call = functools.partial(ctx.run, func, *args, **kwargs)
    return await loop.run_in_executor(None, func_call)


async def run_function(callable_fun,  *args, **kwargs) -> typing.Any:
    if iscoroutinefunction(callable_fun):
        return await callable_fun(*args, **kwargs)
    else:
        return await to_thread(callable_fun, *args, **kwargs)


class Request:
    method = None
    url = None

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def __repr__(self):
        return f"<{self.method} {self.url}>"

    def get_http_kwargs(self):
        return self.__dict__.copy()


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
        self.ok = 1 if self.status < 300 else 0

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


class RequestDownloader:
    def __init__(self, session=None, proxy_fun=None):
        self.downloader_cls = requests
        self.proxy_fun = proxy_fun

        if session:
            self.default_session = session
        else:
            self.default_session = requests.Session()
            self.default_session.trust_env = False

    @staticmethod
    def get_response(resp):
        return Response(
            url=str(resp.url),
            content=resp.content,
            status=resp.status_code,
            cookies=resp.cookies,
            headers=resp.headers,
            history=resp.history
        )

    @staticmethod
    def get_kwargs(kwargs):
        kwargs.pop("session", None)
        _kwargs = kwargs.pop("kwargs")

        new_kwargs = {**kwargs, **_kwargs}
        new_kwargs.pop("self")
        new_kwargs.pop("session", None)
        return new_kwargs

    def fetch(self, url, method="GET", params=None, data=None, headers=None, cookies=None, files=None, auth=None,
              timeout=None, allow_redirects=True, proxies=None, hooks=None, stream=None, verify=None, cert=None,
              json=None, session=None, **kwargs):

        _kwargs = locals().copy()
        _kwargs.pop("self")
        _kwargs.pop("kwargs")
        _kwargs.pop("session")
        if not session:
            session = self.default_session

        resp = session.request(**_kwargs)
        return self.get_response(resp)

    async def async_fetch(self, url, method="GET", params=None, data=None, headers=None, cookies=None, files=None,
                          auth=None, timeout=None, allow_redirects=True, proxies=None, hooks=None, stream=None,
                          verify=None, cert=None, json=None, session=None, **kwargs):
        _kwargs = locals().copy()
        _kwargs.pop("self")
        _kwargs.pop("kwargs")
        return await run_function(self.fetch, **_kwargs, **kwargs)


class HttpxDownloader:
    def __init__(self, session=None, proxy_fun=None, loop=None):
        self.downloader_cls = httpx

        self.proxy_fun = proxy_fun

        if session:
            self.default_session = session
        else:
            self.default_session = httpx.Client(verify=False, http2=False, trust_env=False)
            self.default_async_session = httpx.AsyncClient(verify=False, http2=False, trust_env=False)

        self.loop = loop if loop else asyncio.get_event_loop()

        atexit.register(self.close)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def run_in_async(self, func, *args, **kwargs):
        return self.loop.run_until_complete(func(*args, **kwargs))

    def close(self):
        if self.default_session:
            self.default_session.close()
            self.run_in_async(self.default_async_session.aclose)

    @staticmethod
    def get_response(resp):
        return Response(
            url=str(resp.url),
            content=resp.content,
            status=resp.status_code,
            cookies=resp.cookies,
            headers=resp.headers,
            history=resp.history
        )

    @staticmethod
    def get_kwargs(kwargs):
        kwargs.pop("session", None)
        _kwargs = kwargs.pop("kwargs")

        new_kwargs = {**kwargs, **_kwargs}
        new_kwargs.pop("self")
        new_kwargs.pop("session", None)
        new_kwargs.pop("proxies", None)
        return new_kwargs

    def fetch(self, url, method="GET", params=None, data=None, headers=None, cookies=None, files=None, auth=None,
              timeout=None, allow_redirects=True, content=None, proxies=None, json=None, session=None, **kwargs):
        _kwargs = self.get_kwargs(locals().copy())

        if not session:
            session = self.default_session

        if proxies:
            session.proxies = proxies
        else:
            session.proxies = {"https": None, "http": None}

        resp = session.request(**kwargs)
        return self.get_response(resp)

    async def async_fetch(self, url, method="GET", params=None, data=None, headers=None, cookies=None, files=None,
                          auth=None, timeout=None, allow_redirects=True, content=None, proxies=None, json=None,
                          session=None, **kwargs):
        _kwargs = self.get_kwargs(locals().copy())

        if not session:
            session = self.default_async_session

        if proxies is not None:
            session.proxies = proxies
        else:
            if self.proxy_fun:
                session.proxies = await run_function(self.proxy_fun)
            else:
                session.proxies = {"https": None, "http": None}

        resp = await session.request(**_kwargs)
        return self.get_response(resp)


class AiohttpDownloader:
    def __init__(self, session=None, proxy_fun=None, loop=None):
        self.downloader_cls = aiohttp
        self.tc = None

        if session:
            self.default_session = session
        else:
            self.default_session = None

        self.proxy_fun = proxy_fun

        self.loop = loop if loop else asyncio.get_event_loop()

        atexit.register(self.close)

    @staticmethod
    async def get_response(resp):
        return Response(
            url=str(resp.url),
            content=await resp.read(),
            status=resp.status,
            cookies=resp.cookies,
            headers=resp.headers,
            history=resp.history,
        )

    def run_in_async(self, func, *args, **kwargs):
        return self.loop.run_until_complete(func(*args, **kwargs))

    @staticmethod
    def get_kwargs(kwargs):
        kwargs.pop("session", None)
        _kwargs = kwargs.pop("kwargs")

        new_kwargs = {**kwargs, **_kwargs}
        new_kwargs.pop("self")
        new_kwargs.pop("session", None)
        return new_kwargs

    async def get_session(self):
        if self.default_session:
            return self.default_session
        else:
            jar = aiohttp.DummyCookieJar()
            self.tc = TCPConnector(limit=1000, force_close=True, enable_cleanup_closed=True, ssl=False)
            self.default_session = aiohttp.ClientSession(connector=self.tc, cookie_jar=jar)
            return self.default_session

    def close(self):
        if self.default_session:
            self.run_in_async(self.default_session.close)
            self.tc.close()

    def fetch(self, url, method="GET", headers=None, cookies=None, allow_redirects=True, params=None, data=None,
              json=None, auth=None, timeout=None, ssl=None, proxy=None, session=None, **kwargs):
        _kwargs = self.get_kwargs(locals().copy())
        return self.run_in_async(self.async_fetch, **_kwargs)

    async def async_fetch(self, url, method="GET", headers=None, cookies=None, allow_redirects=True, params=None,
                          data=None, json=None, auth=None, timeout=None, ssl=None, proxy=None, session=None, **kwargs):
        _kwargs = self.get_kwargs(locals().copy())

        if not session:
            session = await self.get_session()

        if proxy is not None and self.proxy_fun:
            _kwargs["proxy"] = await run_function(self.proxy_fun)

        async with aiohttp.ClientSession() as client:
            async with client.request(**_kwargs) as resp:
                return await self.get_response(resp)



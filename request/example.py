import asyncio

from request.request_util import RequestDownloader, HttpxDownloader, AiohttpDownloader


class TestMiddleware:
    def process_request(self, request, download_ins):
        print("Middleware", request)

    def process_response(self, request, response, download_ins):
        print("Middleware", response)


def test_all():
    loop = asyncio.get_event_loop()

    for cls in [RequestDownloader, HttpxDownloader, AiohttpDownloader]:
        print(f"########################### {cls.__name__} ###########################")
        downloader = RequestDownloader(middlewares=[TestMiddleware])

        result = downloader.get("https://httpbin.org/get")
        print("get===================", result)

        result = downloader.post("https://httpbin.org/post")
        print("post===================", result)

        result = downloader.download("https://httpbin.org/get")
        print("download get===================", result)

        result = downloader.download("https://httpbin.org/post", method="POST")
        print("download post===================", result)

        result = downloader.fetch("https://httpbin.org/get")
        print("fetch===================", result)

        result = downloader.fetch("https://httpbin.org/post", method="POST")
        print("fetch post===================", result)

        result = loop.run_until_complete(downloader.async_get("https://httpbin.org/get"))
        print("async_get===================", result)

        result = loop.run_until_complete(downloader.async_post("https://httpbin.org/post"))
        print("async_post===================", result)

        result = loop.run_until_complete(downloader.async_download("https://httpbin.org/get"))
        print("async_download get===================", result)

        result = loop.run_until_complete(downloader.async_download("https://httpbin.org/post", method="POST"))
        print("async_download post===================", result)

        result = loop.run_until_complete(downloader.async_fetch("https://httpbin.org/get"))
        print("async_fetch get===================", result)

        result = loop.run_until_complete(downloader.async_fetch("https://httpbin.org/post", method="POST"))
        print("async_fetch post===================", result)


def example1():
    downloader = RequestDownloader()
    response = downloader.get("https://httpbin.org/html")
    title = response.xpath("//h1/text()").get()
    print(title)


def example2():
    downloader = RequestDownloader()
    response = downloader.get("https://httpbin.org/json")
    data = response.json()
    title = data["slideshow"]["title"]
    print(title)


def example3():
    downloader = AiohttpDownloader()
    response = downloader.get("https://httpbin.org/json")
    data = response.json()
    title = data["slideshow"]["title"]
    print(title)


async def example4():
    downloader = RequestDownloader()
    response = await downloader.async_get("https://httpbin.org/json")
    data = response.json()
    title = data["slideshow"]["title"]
    print(title)


if __name__ == '__main__':
    # test_all()
    example1()
    example2()
    example3()
    asyncio.get_event_loop().run_until_complete(example4())

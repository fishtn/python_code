import asyncio
import time

from request_util import RequestDownloader, HttpxDownloader, AiohttpDownloader


async def get_proxy():
    # return ""
    return "http://127.0.0.1:7890"


async def demo():
    download = RequestDownloader(proxy_fun=get_proxy)
    start_time = time.perf_counter()
    tasks = [download.async_fetch("https://www.baidu.com") for _ in range(100)]
    results = await asyncio.gather(*tasks)
    status_list = [r.status for r in results]
    end_time = time.perf_counter()
    print(f"requsts  状态码为200的个数：{status_list.count(200)} 花费时间：{end_time - start_time}")


async def demo2():
    download = HttpxDownloader(proxy_fun=get_proxy)
    start_time = time.perf_counter()
    tasks = [download.async_fetch("https://www.baidu.com") for _ in range(100)]
    results = await asyncio.gather(*tasks)
    status_list = [r.status for r in results]
    end_time = time.perf_counter()
    print(f"Httpx  状态码为200的个数：{status_list.count(200)} 花费时间：{end_time - start_time}")


async def demo3():
    download = AiohttpDownloader(proxy_fun=get_proxy)
    start_time = time.perf_counter()
    tasks = [download.async_fetch("https://www.baidu.com") for _ in range(100)]
    results = await asyncio.gather(*tasks)
    status_list = [r.status for r in results]
    end_time = time.perf_counter()
    print(f"Aiohttp  状态码为200的个数：{status_list.count(200)} 花费时间：{end_time - start_time}")


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(demo())
    loop.run_until_complete(demo2())
    loop.run_until_complete(demo3())

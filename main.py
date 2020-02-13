import asyncio
import aiofiles
import aiofile
import concurrent
import enum
import pathlib
import random
import string
import time
import shutil
from contextlib import closing, contextmanager
from functools import partial


def delete_dir(dirname: str):
    dirpath = pathlib.Path(f'./{dirname}')
    #print(f'delete dir `{dirpath}`')
    if dirpath.exists():
        shutil.rmtree(dirpath)


def make_dir(dirname: str):
    dirpath = pathlib.Path(f'./{dirname}')
    #print(f'make dir `{dirpath}`')
    dirpath.mkdir(parents=True, exist_ok=True)


def random_string(count=5, chars=string.ascii_uppercase + string.digits) -> str:
    return ''.join(random.choices(chars, k=count))


@contextmanager
def perf_manager(description):
    start_time = time.perf_counter()
    try:
        yield
    finally:
        finish_time = time.perf_counter()
        time_delta = (finish_time - start_time) * 10 ** 3
        print(f'{description}: {time_delta:.6f} ms')


def timeit(description, sync=True):
    def decorator(func):
        def wrapper(*args, **kwargs):
            with perf_manager(description):
                return func(*args, **kwargs)

        async def wrapper_async(*args, **kwargs):
            with perf_manager(description):
                return await func(*args, **kwargs)

        return wrapper if sync else wrapper_async
    return decorator


def generate_random_chunks(n: int, chunk_size: int = 1024) -> list:
    return [random_string(chunk_size) for _ in range(n)]   


class Dirs(enum.Enum):
    AIOFILES = 'aiofiles'
    AIOFILE = 'aiofile'
    FILE = 'file'
    EXECUTOR = 'executor'


@timeit(f'{Dirs.AIOFILES}', sync=False)
async def test_aiofiles(chunks: list):
    dir_name = Dirs.AIOFILES.value
    for idx, chunk in enumerate(chunks):
        async with aiofiles.open(f'./{dir_name}/{idx}.txt', 'w+') as f:
            await f.write(chunk)


@timeit(f'{Dirs.AIOFILE} ', sync=False)
async def test_aiofile(chunks: list):
    dir_name = Dirs.AIOFILE.value
    for idx, chunk in enumerate(chunks):
        async with aiofile.AIOFile(f'./{dir_name}/{idx}.txt', 'w+') as f:
            await f.write(chunk)


def write_file(file_name, data):
    with open(file_name, 'w+') as f:
        f.write(data)


@timeit(f'{Dirs.FILE}    ', sync=True)
def test_file(chunks: list):
    dir_name = Dirs.FILE.value
    for idx, chunk in enumerate(chunks):
        write_file(f'./{dir_name}/{idx}.txt', chunk)


# Create a limited thread pool for blockinkg i/o operations.
executor = concurrent.futures.ThreadPoolExecutor(
    max_workers=3
)


@timeit(f'{Dirs.EXECUTOR}', sync=False)
async def test_executor(chunks: list):
    ioloop = asyncio.get_event_loop()
    dir_name = Dirs.EXECUTOR.value
    coros = [
        ioloop.run_in_executor(executor, partial(write_file, f'./{dir_name}/{idx}.txt', chunk))
        for idx, chunk in enumerate(chunks)
    ]
    await asyncio.gather(*coros)


async def test(chunks):

    for dirname in Dirs:
        delete_dir(dirname.value)
        make_dir(dirname.value)

    await test_aiofiles(chunks)
    await test_aiofile(chunks)
    await test_executor(chunks)
    test_file(chunks)

    for dirname in Dirs:
        delete_dir(dirname.value)


def main():
    ioloop = asyncio.get_event_loop()
    n, chunk_size = 10, 1024 * 10
    print(f'n={n}, chunk_size={chunk_size}')
    chunks = generate_random_chunks(n, chunk_size)
    try:
        ioloop.run_until_complete(test(chunks))
    finally:
        # Shutting down and closing file descriptors after interrupt
        ioloop.run_until_complete(ioloop.shutdown_asyncgens())
        ioloop.close()
        print('Exited')


if __name__ == '__main__':
    main()


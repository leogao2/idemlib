import idemlib
import idemlib.hashers


import random

def some_random_fn(i: int):
    return random.randint(0, 1000000)


def test_hasher():
    # TODO: more comprehensive tests of hasher
    hasher = idemlib.hashers.ObjectHasherV1()

    assert hasher.hash_obs(None) == hasher.hash_obs(None)

    assert hasher.hash_obs([1, 2, 3]) == hasher.hash_obs([1, 2, 3])
    assert hasher.hash_obs([1, 2, 3]) != hasher.hash_obs([1, 2, 4])

    assert hasher.hash_obs({}) == hasher.hash_obs({})
    assert hasher.hash_obs({}) != hasher.hash_obs({"a": 1})

    assert hasher.hash_obs((1, 2)) == hasher.hash_obs((1, 2))
    assert hasher.hash_obs((1, 2)) != hasher.hash_obs((1, 3))

    assert hasher.hash_obs(idemlib.CacheHelper) == hasher.hash_obs(idemlib.CacheHelper)
    assert hasher.hash_obs(idemlib.CacheHelper) != hasher.hash_obs(idemlib.hashers.ObjectHasherV1)

    assert hasher.hash_obs(slice(1, 2)) == hasher.hash_obs(slice(1, 2))
    assert hasher.hash_obs(slice(1, 2)) != hasher.hash_obs(slice(1, 3))

    assert hasher.hash_obs(b"\0") == hasher.hash_obs(b"\0")
    assert hasher.hash_obs(b"\0") != hasher.hash_obs(b"\1")


def test_cache_basic():
    cache = idemlib.CacheHelper(None)
    cached_some_random_fn = cache("test")(some_random_fn)

    assert some_random_fn(1) != some_random_fn(1)
    assert cached_some_random_fn(1) == cached_some_random_fn(1)
    assert cached_some_random_fn(1) != cached_some_random_fn(2)


def test_cache_batched():
    xs = list(range(100))

    cache = idemlib.CacheHelper(None)

    def _fn(xs):
        return [some_random_fn(x) for x in xs]
    
    cached_fn = cache.each("test")(_fn)

    assert _fn(xs) != cached_fn(xs)
    assert cached_fn(xs) == cached_fn(xs)

    assert cached_fn(xs[:10]) == cached_fn(xs[:10])
    assert cached_fn(xs[:10]) == cached_fn(xs[:5]) + cached_fn(xs[5:10])

    permutation = list(range(100))
    random.shuffle(permutation)

    assert cached_fn([
        xs[i] for i in permutation
    ]) == [
        cached_fn(xs)[i] for i in permutation
    ]

    xs2 = list(range(100, 200))

    assert cached_fn(xs2[:10]) == cached_fn(xs2[:10])
    assert cached_fn(xs2[:12]) == cached_fn(xs2[:5]) + cached_fn(xs2[5:12])
    assert cached_fn(xs2[1:15]) == cached_fn(xs2[1:7]) + cached_fn(xs2[7:15])

    assert cached_fn([201, 201]) == cached_fn([201, 201])
    assert cached_fn([202, 202]) == cached_fn([202]) + cached_fn([202])
    assert cached_fn([203, 203, 203, 204]) == cached_fn([203, 203]) + cached_fn([203, 204])


def test_cache_async():
    async def test_fn(i: int):
        return random.randint(0, 1000000)

    cache = idemlib.CacheHelper(None)

    cached_test_fn = cache("test")(test_fn)

    import asyncio

    assert asyncio.run(cached_test_fn(1)) == asyncio.run(cached_test_fn(1))
    ret1 = asyncio.run(cached_test_fn(1))

    def test_fn2(i: int):
        return random.randint(0, 1000000)
    
    cached_test_fn = cache("test")(test_fn2)

    assert cached_test_fn(1) == cached_test_fn(1)
    assert cached_test_fn(1) != cached_test_fn(2)
    assert cached_test_fn(1) == ret1
    ret2 = cached_test_fn(2)

    cached_test_fn = cache("test")(test_fn)

    assert asyncio.run(cached_test_fn(2)) == ret2

    # TODO: test non-coroutine awaitables


def test_local_file_cache():
    # get temporary directory
    import tempfile
    import os

    with tempfile.TemporaryDirectory() as tmpdirname:
        cache = idemlib.CacheHelper(tmpdirname)

        def test_fn(i: int):
            return random.randint(0, 1000000)
        
        cached_test_fn = cache("test")(test_fn)

        assert cached_test_fn(1) == cached_test_fn(1)
        assert cached_test_fn(1) != cached_test_fn(2)

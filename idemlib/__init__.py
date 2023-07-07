import inspect
import pickle
import re
import time
from collections import defaultdict
from functools import partial, wraps
from typing import Any, Callable, Iterable, Optional, TypeVar, Union, Coroutine, Awaitable

import blobfile as bf

import idemlib.hashers as hashers

T = TypeVar("T")

# either callable or coroutine
F = TypeVar("F", Callable[..., Any], Coroutine[Any, Any, Any], Awaitable[Any])

# TODO: hierarchical cache
class CacheHelper:
    """
    CacheHelper helps cache the return values of function calls based on their
    arguments. It is useful for caching the results of expensive computations
    or API calls.

    CacheHelper hashes the arguments to a function and the cache key defined in
    `@cache`, but *not* the function name or function implementation. This
    allows the user to rename, move, or change the implementation of the
    function without invalidating the cache. When the implementation of the
    function changes in ways that affect the output, the user is responsible for
    changing the cache key.

    By convention, the cache key is a random hex string, with a version number
    at the end (such as "976d37ab_0"). This is to avoid collisions and ensure
    that old names are not left in strings, causing confusion. The version
    number makes it easy to invalidate the cache.

    CacheHelper should "just work" for async functions.

    For custom classes, CacheHelper will error by default to be safe -- to add
    support for custom classes, add a function to `special_hashing` that takes
    the object and returns a hashable object. If the custom class has attributes
    that could themselves not be hashable, the function should recursively call
    `cache._prepare_for_hash` on those attributes.

    Usage:

    ```
    cache = CacheHelper("az://my/container")

    @cache("976d37ab_0")
    def myfunc(x, y):
        print("running myfunc")
        return x + y


    myfunc(1, 2) # prints "running myfunc" and returns 3
    myfunc(1, 2) # prints nothing and returns 3
    myfunc(1, 2, cache_version=1) # prints "running myfunc" and returns 3
    ```
    """

    def __init__(self, save_location: Optional[str], object_hasher=None):
        if save_location is None:
            self.save_location = None
            # memory
            self._cache = {}
        else:
            assert isinstance(save_location, str)
            self.save_location = save_location if save_location[-1] != "/" else save_location[:-1]
        
        self.object_hasher = object_hasher or hashers.ObjectHasherV1()

        # for backwards compatibility
        self.special_hashing = self.object_hasher.special_hashing

    def _get_kv(self, key: str):
        if self.save_location is None:
            return self._cache[key]

        try:
            return pickle.load(bf.BlobFile(self.save_location + "/" + key, "rb"))
        except (FileNotFoundError, EOFError):
            raise KeyError(key)

    def _set_kv(self, key: str, value):
        if self.save_location is None:
            self._cache[key] = value
            return

        with bf.BlobFile(self.save_location + "/" + key, "wb") as f:
            pickle.dump(value, f)

    def _update_source_cache(self, fname, lineno, new_key):
        assert "/tmp/ipykernel" not in fname, "Can't use @cache autofill in a notebook!"

        with open(fname, "r") as f:
            file_lines = f.read().split("\n")

        # line numbering is 1-indexed
        lineno -= 1

        s = re.match(r"@((?:[^\W0-9]\w*\.)?)cache(\(\))?", file_lines[lineno].lstrip())
        assert s, "@cache can only be used as a decorator!"
        leading_whitespace = re.match(r"^\s*", file_lines[lineno]).group(0)

        file_lines[lineno] = f'{leading_whitespace}@{s.group(1)}cache("{new_key}")'

        with open(fname, "w") as f:
            f.write("\n".join(file_lines))
    
    def hash_obs(self, *args, **kwargs):
        # for backwards compatibility
        return self.object_hasher.hash_obs(*args, **kwargs)
    
    def each(self, key=None):
        def wrapper(fn, self, key):
            @wraps(fn)
            def _fn(it: list[T], **kwargs: Any) -> list[T]:
                _sentinel = object()
                ret = []
                to_run = []
                to_run_hashes = []
                to_run_hashes_orig_inds = defaultdict(list)
                for i, x in enumerate(it):
                    hash = self.hash_obs(x, **kwargs)
                    overall_input_hash = key + "_" + hash
                    try:
                        ob = self._read_cache_data(fn, overall_input_hash)
                        ret.append(ob)
                    except KeyError:
                        if not to_run_hashes_orig_inds[hash]:
                            to_run.append(x)
                            to_run_hashes.append(hash)
                        to_run_hashes_orig_inds[hash].append(i)
                        ret.append(_sentinel)
                    
                res = fn(to_run, **kwargs) if to_run else []

                for y, h in zip(res, to_run_hashes):
                    overall_input_hash = key + "_" + h
                    self._write_cache_data(fn, overall_input_hash, y)

                    for ind in to_run_hashes_orig_inds[h]:
                        assert ret[ind] is _sentinel
                        ret[ind] = y
                
                assert all(x is not _sentinel for x in ret)

                return ret
            return _fn

        return partial(wrapper, key=key, self=self)
        

    def __call__(self, key=None, *, _callstackoffset=2) -> Callable[[F], F]:
        def wrapper(fn: F, self, key, _callstackoffset) -> F:
            # execution always gets here, before the function is called

            if not callable(fn):
                return wrapper(lambda: fn, self, key, _callstackoffset)()

            try:
                fn.__annotations__["cache_version"] = Any
                fn.__annotations__["cache_disable"] = bool
            except AttributeError:
                pass

            if key is None:
                key = self.hash_obs(fn.__module__, fn.__name__, inspect.getsource(fn))[:8] + "_0"
                # the decorator part of the stack is always the same size because we only get here if key is None
                stack_original_function = inspect.stack()[_callstackoffset]
                self._update_source_cache(
                    stack_original_function.filename, stack_original_function.lineno - 1, key
                )

            @wraps(fn)
            def _fn(*args, **kwargs):
                # execution gets here only after the function is called
                
                if kwargs.pop(kwargs.pop("_idemlib_disable_kwarg", "cache_disable"), False):
                    return fn(*args, **kwargs)
                
                arg_hash = self.hash_obs(*args, **kwargs)

                kwargs.pop(kwargs.pop("_idemlib_nonce_kwarg", "cache_version"), None)
                

                overall_input_hash = key + "_" + arg_hash

                try:
                    return self._read_cache_data(fn, overall_input_hash)
                except KeyError:
                    start_time = time.time()
                    ret = fn(*args, **kwargs)
                    end_time = time.time()

                    return self._write_cache_data(
                        fn,
                        overall_input_hash,
                        ret,
                        start_time=start_time,
                        end_time=end_time,
                    )

            return _fn

        if callable(key):
            return wrapper(fn=key, self=self, key=None, _callstackoffset=_callstackoffset)

        return partial(wrapper, self=self, key=key, _callstackoffset=_callstackoffset)

    def _write_cache_data(self, fn, key, ret, **kwargs):
        ## ASYNC HANDLING, first run
        if inspect.isawaitable(ret):
            async def _wrapper(ret):
                # turn the original async function into a synchronous one and return a new async function
                ret = await ret
                self._set_kv(
                    key,
                    {
                        "ret": ret,
                        "awaitable": True,
                        "iscoroutine": inspect.iscoroutinefunction(fn),
                        **kwargs,
                    },
                )
                return ret

            return _wrapper(ret)
        else:
            self._set_kv(
                key,
                {
                    "ret": ret,
                    "awaitable": False,
                    "iscoroutine": inspect.iscoroutinefunction(fn),
                    **kwargs,
                },
            )
            return ret

    def _read_cache_data(self, fn, key):
        ob = self._get_kv(key)
        ret = ob["ret"]
        original_awaitable = ob["awaitable"]
        original_was_coroutine = ob["iscoroutine"]
        current_is_coroutine = inspect.iscoroutinefunction(fn)

        ## ASYNC HANDLING, resume from file

        if original_was_coroutine and current_is_coroutine:
            return_awaitable = True  # coroutine -> coroutine
        elif original_was_coroutine and not current_is_coroutine:
            return_awaitable = False  # coroutine -> normal
        elif (
            not original_was_coroutine
            and not original_awaitable
            and current_is_coroutine
        ):
            return_awaitable = True  # normal -> coroutine
        elif (
            not original_was_coroutine
            and not original_awaitable
            and not current_is_coroutine
        ):
            return_awaitable = False  # normal -> normal
        elif not original_was_coroutine and original_awaitable and current_is_coroutine:
            return_awaitable = True  # normal_returning_awaitable -> coroutine
        elif (
            not original_was_coroutine
            and original_awaitable
            and not current_is_coroutine
        ):
            # this case is ambiguous! we can't know if the modifier function returns an awaitable or not
            # without actually running the function, so we just assume it's an awaitable,
            # since probably nothing changed.
            return_awaitable = (
                True  # normal_returning_awaitable -> normal/normal_returning_awaitable
            )
        else:
            return_awaitable = False  # fallback - most likely this is a bug
            print(f"WARNING: unknown change in async situation for {fn._name__}")

        if return_awaitable:

            async def _wrapper(ret):
                # wrap ret in a dummy async function
                return ret

            return _wrapper(ret)
        else:
            return ret



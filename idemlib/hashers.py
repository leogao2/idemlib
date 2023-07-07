
import asyncio
import dataclasses
from decimal import Decimal
from fractions import Fraction
from functools import reduce
import hashlib
import json
import struct
import types
from typing import Any, Callable, Dict, Optional, Type, Union
from datetime import date, datetime, time, timedelta
from uuid import UUID

class ObjectHasherV1:
    def __init__(self):
        self.special_hashing: Dict[Union[Type, str], Callable[[Any], Any]] = {}

        self.special_hashing[list] = lambda x: list(map(self._prepare_for_hash, x))
        self.special_hashing[dict] = lambda x: {
            _dict_key(self._prepare_for_hash(k)): self._prepare_for_hash(v) for k, v in x.items()
        }
        self.special_hashing[tuple] = lambda x: tuple(map(self._prepare_for_hash, x))
        # self.special_hashing[types.FunctionType] = lambda x: (
        #     "function",
        #     x.__name__,
        # )  # TODO: better semantics
        self.special_hashing[type] = lambda x: ("type", x.__name__)
        self.special_hashing[slice] = lambda x: ("slice", x.start, x.stop, x.step)
        self.special_hashing[bytes] = lambda x: ("bytes", hashlib.sha256(x).hexdigest())
        self.special_hashing[bytearray] = lambda x: ("bytearray", hashlib.sha256(x).hexdigest())
        self.special_hashing[complex] = lambda x: ("complex", x.real, x.imag)
        self.special_hashing[date] = lambda x: ("date", x.isoformat())
        self.special_hashing[datetime] = lambda x: ("datetime", x.date().isoformat())
        self.special_hashing[time] = lambda x: ("time", x.isoformat())
        self.special_hashing[timedelta] = lambda x: ("timedelta", x.total_seconds())
        self.special_hashing[Decimal] = lambda x: ("decimal", str(x))
        self.special_hashing[UUID] = lambda x: ("uuid", str(x)) 
        self.special_hashing[Fraction] = lambda x: ("fraction", x.numerator, x.denominator)

        # lazy hashing - importing all these packages may be slow or they may not be installed
        self.special_hashing["torch.Tensor"] = lambda x: (
            "torch.Tensor",
            x.tolist(),
            self._prepare_for_hash(x.dtype),
            self._prepare_for_hash(x.device),
        )
        self.special_hashing["torch.dtype"] = lambda x: ("torch.dtype", str(x))
        self.special_hashing["torch.device"] = lambda x: ("torch.device", str(x))
        self.special_hashing["torch.nn.modules.module.Module"] = lambda x: ("torch.nn.Module", self._prepare_for_hash(x.state_dict()))

        self.special_hashing["pyspark.rdd.RDD"] = lambda x: ("pyspark.rdd.RDD", self._hash_rdd(x))
        self.special_hashing["pyspark.SparkContext"] = lambda x: (
            "pyspark.SparkContext",
            x.applicationId,
            x.master,
        )

        self.special_hashing["numpy.ndarray"] = lambda x: (
            "numpy.ndarray",
            x.tolist(),
            self._prepare_for_hash(x.dtype),
        )
        self.special_hashing["numpy.dtype"] = lambda x: ("numpy.dtype", str(x))

        # idemlib objects. The reason we _don't_ hash the cache location is that
        # semantically, the cache should be totally transparent, and you're not supposed
        # to use the cache location to store different copies of the thing that you want to tell apart later.
        self.special_hashing["idemlib.CacheHelper"] = lambda x: ("idemlib.CacheHelper", self._prepare_for_hash(x.object_hasher)) # would require circular imports
        self.special_hashing["idemlib.hashers.ObjectHasherV1"] = lambda x: ("idemlib.hashers.ObjectHasherV1",)

        self.special_hashing["tiktoken.core.Encoding"] = lambda x: ("tiktoken.core.Encoding", x.name)

        self.special_hashing["cdict.main.cdict"] = lambda x: ("cdict.C", self._prepare_for_hash(list(x)))
        # self.id_to_hash = {} # object id -> hash

    def _prepare_for_hash(self, x):
        # if id(x) in self.id_to_hash:
        #     return self.id_to_hash[id(x)]

        if hasattr(x, "__idemlib_hash__"):
            return x.__idemlib_hash__(self)

        superclasses = [_fullname(x) for x in x.__class__.__mro__]
        for type_, fn in self.special_hashing.items():
            if isinstance(type_, str):
                if type_ in superclasses:
                    return fn(x)
                continue

            if isinstance(x, type_):
                return fn(x)

        if dataclasses.is_dataclass(x):
            return self._prepare_for_hash(dataclasses.asdict(x) | {"__dataclass__": x.__class__.__name__})

        # self.id_to_hash[id(x)] = x
        return x

    _local_rdd_hash_cache = {}

    def _hash_rdd(self, rdd):
        key = (rdd.context.applicationId, rdd.id())
        if key in self._local_rdd_hash_cache:
            return self._local_rdd_hash_cache[key]

        hash = rdd.map(self._prepare_for_hash).reduce(lambda x, y: self._prepare_for_hash((x, y)))
        self._local_rdd_hash_cache[key] = hash
        return hash

    class _ObjectEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(
                obj,
                (
                    asyncio.Lock,
                    asyncio.Event,
                    asyncio.Condition,
                    asyncio.Semaphore,
                    asyncio.BoundedSemaphore,
                ),
            ):
                # don't do anything with asyncio objects
                return None

            return super().default(obj)

    def hash_obs(self, *args, **kwargs):
        x = [
            [self._prepare_for_hash(i) for i in args],
            [
                (self._prepare_for_hash(k), self._prepare_for_hash(v))
                for k, v in list(sorted(kwargs.items()))
            ],
        ]

        jsonobj = json.dumps(x, sort_keys=True, cls=self._ObjectEncoder)
        arghash = hashlib.sha256(jsonobj.encode()).hexdigest()
        return arghash

def _fullname(klass):
    module = klass.__module__
    return module + "." + klass.__qualname__

def hash_object(ob):
    return ObjectHasherV1().hash_obs(ob)

def _dict_key(x):
    # tuples can't be the key of a json dict
    if isinstance(x, tuple):
        return str(x)
    return x

# TODO for v2:
# - make torch/numpy hashing more efficient
# - better function hashing
# - map args to kwargs, so that equivalent function calls don't get hashed differently
# - merkel tree instead of json
# - lazy import superclasses handling

from xxhash import xxh64


class ObjectHasherV2Candidate1:
    def __init__(self):
        self.special_hashing: Dict[Union[Type, str], Callable[[Any], Any]] = {}

        # self.special_hashing[types.FunctionType] = lambda x: (
        #     "function",
        #     x.__name__,
        # )  # TODO: better semantics
        self.special_hashing[type] = lambda x: ("type", x.__name__)
        self.special_hashing[slice] = lambda x: ("slice", x.start, x.stop, x.step)
        self.special_hashing[bytes] = lambda x: ("bytes", xxh64(x).hexdigest())
        self.special_hashing[bytearray] = lambda x: ("bytearray", xxh64(x).hexdigest())
        self.special_hashing[complex] = lambda x: ("complex", x.real, x.imag)
        self.special_hashing[date] = lambda x: ("date", x.isoformat())
        self.special_hashing[datetime] = lambda x: ("datetime", x.date().isoformat())
        self.special_hashing[time] = lambda x: ("time", x.isoformat())
        self.special_hashing[timedelta] = lambda x: ("timedelta", x.total_seconds())
        self.special_hashing[Decimal] = lambda x: ("decimal", str(x))
        self.special_hashing[UUID] = lambda x: ("uuid", str(x)) 
        self.special_hashing[Fraction] = lambda x: ("fraction", x.numerator, x.denominator)

        # lazy hashing - importing all these packages may be slow or they may not be installed
        # self.special_hashing["torch.Tensor"] = lambda x: (
        #     "torch.Tensor",
        #     x.tolist(),
        #     x.dtype,
        #     x.device,
        # )
        self.special_hashing["torch.dtype"] = lambda x: ("torch.dtype", str(x))
        self.special_hashing["torch.device"] = lambda x: ("torch.device", str(x))
        self.special_hashing["torch.nn.Module"] = lambda x: ("torch.nn.Module", x.state_dict())

        self.special_hashing["pyspark.rdd.RDD"] = lambda x: ("pyspark.rdd.RDD", self._hash_rdd(x))
        self.special_hashing["pyspark.SparkContext"] = lambda x: (
            "pyspark.SparkContext",
            x.applicationId,
            x.master,
        )

        # self.special_hashing["numpy.ndarray"] = lambda x: (
        #     "numpy.ndarray",
        #     x.tolist(),
        #     x.dtype),
        # )
        self.special_hashing["numpy.dtype"] = lambda x: ("numpy.dtype", str(x))

        # idemlib objects. The reason we _don't_ hash the cache location is that
        # semantically, the cache should be totally transparent, and you're not supposed
        # to use the cache location to store different copies of the thing that you want to tell apart later.
        self.special_hashing["idemlib.CacheHelper"] = lambda x: ("idemlib.CacheHelper", x.object_hasher) # would require circular imports
        self.special_hashing["idemlib.hashers.ObjectHasherV1"] = lambda x: ("idemlib.hashers.ObjectHasherV1",)
        self.special_hashing["idemlib.hashers.ObjectHasherV2"] = lambda x: ("idemlib.hashers.ObjectHasherV2",)

        self._id_to_hash = {}
    
    def hash(self, ob, strict=True):
        do_cache = type(ob) not in [
            int, float, bool, type(None)
        ] and not (isinstance(ob, str) and len(ob) < 1000) # don't cache small objects' hashes
        if do_cache and id(ob) in self._id_to_hash:
            return self._id_to_hash[id(ob)]

        hash = self._hash(ob, strict=strict)

        if do_cache:
            self._id_to_hash[id(ob)] = hash

        return hash

    def _hash(self, ob, strict=True) -> str:
        # basic types
        if isinstance(ob, int):
            s = b"i" + struct.pack("q", ob)
            if not strict: return s.hex()
            return xxh64(s).hexdigest()

        if isinstance(ob, float):
            s = b"f" + struct.pack("d", ob)
            if not strict: return s.hex()
            return xxh64(s).hexdigest()

        if isinstance(ob, bool):
            s = b"b" + struct.pack("?", ob)
            if not strict: return s.hex()
            return xxh64(s).hexdigest()

        if isinstance(ob, type(None)):
            return xxh64(b"None\0").hexdigest()
        
        if isinstance(ob, str):
            return xxh64(b's' + ob.encode("utf-8")).hexdigest()

        # dataclasses
        if dataclasses.is_dataclass(ob):
            return self.hash(dataclasses.asdict(ob))
            
        # check if it's one of the common containers
        if isinstance(ob, (list, tuple)):
            # use reduce so we don't potentially blow up our memory usage
            xxh64_hash = xxh64()
            xxh64_hash.update("list" if isinstance(ob, list) else "tuple")
            for item in ob:
                xxh64_hash.update(self.hash(item, strict=False))
            return xxh64_hash.hexdigest()

        if isinstance(ob, dict):
            # get items with canonical key ordering
            return self.hash("dict" + reduce(
                self._associative_commutative_hash,
                map(self.hash, ob.items()),
                "0"
            ))

        if isinstance(ob, (set, frozenset)):
            return self.hash((
                "set" if isinstance(ob, set) else "frozenset"
            ) + reduce(
                self._associative_commutative_hash,
                map(self.hash, ob),
                "0",
            ))

        # check for __getstate__
        if hasattr(ob, "__getstate__"):
            return self.hash(ob.__getstate__())

        # try special hashing
        try:
            return self._maybe_special_hashing(ob)
        except TypeError:
            pass

        # finally, try serializing to json
        try:
            return self._json_serialize_hash(ob)
        except (TypeError, OverflowError):
            raise TypeError("Cannot hash object of type %s" % type(ob))
        
    def _json_serialize_hash(self, ob):
        jsonobj = json.dumps(ob, sort_keys=True)
        ret = xxh64(jsonobj.encode()).hexdigest()

        return ret

    def hash_obs(self, *args, **kwargs):
        # for backwards compatibility
        # print(args, kwargs)
        # print(self.hash([args, kwargs]))
        return self.hash([args, kwargs])
    
    def _maybe_special_hashing(self, ob):
        type_str = _fullname(ob.__class__)

        superclasses = [type_str] # TODO
        for type_, fn in self.special_hashing.items():
            if isinstance(type_, str):
                if type_ in superclasses:
                    return fn(ob)
                continue

            if isinstance(ob, type_):
                return fn(ob)
        
        raise TypeError("no special hashing for type %s" % type_str)
    
    _local_rdd_hash_cache = {}

    def _hash_rdd(self, rdd):
        key = (rdd.context.applicationId, rdd.id())
        if key in self._local_rdd_hash_cache:
            return self._local_rdd_hash_cache[key]

        # we assume rdd order does not matter
        hash = rdd.map(self._hash).reduce(lambda x, y: self._associative_commutative_hash(x, y))
        self._local_rdd_hash_cache[key] = hash
        return hash

    @staticmethod
    def _associative_commutative_hash(x: str, y: str) -> str:
        # parse as hex strings
        i = int(x, 16)
        j = int(y, 16)

        ret = i ^ j

        return hex(ret)[2:]

    @staticmethod
    def _list_reduce(x: str, y: str) -> str:
        return hashlib.sha256((x + y).encode()).hexdigest()

    def _prepare_for_hash(self, x):
        # for backwards compatibility
        return x

def foldl(f, xs, init):
    for x in xs:
        init = f(init, x)
    return init


if __name__ == "__main__":
    import IPython; IPython.embed()
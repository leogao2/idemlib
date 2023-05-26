
import asyncio
import dataclasses
from decimal import Decimal
from fractions import Fraction
import hashlib
import json
import types
from typing import Any, Callable, Dict, Type, Union
from datetime import date, datetime, time, timedelta
from uuid import UUID

class ObjectHasherV1:
    def __init__(self):
        self.special_hashing: Dict[Union[Type, str], Callable[[Any], Any]] = {}

        self.special_hashing[list] = lambda x: list(map(self._prepare_for_hash, x))
        self.special_hashing[dict] = lambda x: {
            self._prepare_for_hash(k): self._prepare_for_hash(v) for k, v in x.items()
        }
        self.special_hashing[tuple] = lambda x: tuple(map(self._prepare_for_hash, x))
        self.special_hashing[types.FunctionType] = lambda x: (
            "function",
            x.__name__,
        )  # TODO: better semantics
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

    def _prepare_for_hash(self, x):
        type_str = _fullname(x.__class__)

        superclasses = [type_str]
        for type_, fn in self.special_hashing.items():
            if isinstance(type_, str):
                if type_ in superclasses:
                    return fn(x)
                continue

            if isinstance(x, type_):
                return fn(x)

        if dataclasses.is_dataclass(x):
            return self._prepare_for_hash(dataclasses.asdict(x))

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
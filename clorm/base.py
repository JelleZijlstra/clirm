from __future__ import annotations
from dataclasses import dataclass
import enum
import sqlite3
import weakref
from typing import (
    ClassVar,
    Generic,
    Mapping,
    TypeVar,
    Self,
    Any,
    Never,
    get_origin,
    get_args,
    Union,
)
from types import UnionType, NoneType

T = TypeVar("T")


class DoesNotExist(Exception):
    """Raised when trying to access an object that does not exist."""


@dataclass
class Clorm:
    conn: sqlite3.Connection

    def select_one(
        self, query: str, parameters: tuple[Any, ...] = ()
    ) -> Mapping[str, Any] | None:
        cursor = self.conn.cursor()
        cursor.row_factory = sqlite3.Row
        res = cursor.execute(query, parameters)
        return res.fetchone()

    def execute(self, query: str, parameters: tuple[Any, ...] = ()) -> sqlite3.Cursor:
        cursor = self.conn.cursor()
        res = cursor.execute(query, parameters)
        self.conn.commit()
        return res


class Field(Generic[T]):
    name: str
    _typ: type[T]
    _allow_none: bool

    def __init__(self, name: str | None = None) -> None:
        if name is not None:
            self.name = name

    def __set_name__(self, owner: object, name: str) -> None:
        if not hasattr(self, "name"):
            self.name = name

    def __get__(self, obj: Model | None, objtype: object = None) -> T:
        if obj is None:
            return self
        raw_value = self.get_raw(obj)
        return self.deserialize(raw_value)

    def deserialize(self, raw_value: Any) -> T:
        if self.allow_none and raw_value is None:
            return None
        return self.typ(raw_value)

    def get_raw(self, obj: Model) -> Any:
        if self.name not in obj._clorm_data:
            obj.load()
        return obj._clorm_data[self.name]

    def __set__(self, obj: Model, value: T) -> None:
        raw_value = self.serialize(value)
        self.set_raw(obj, raw_value)

    def serialize(self, value: T) -> Any:
        if self.allow_none and value is None:
            return None
        return value

    def set_raw(self, obj: Model, value: Any) -> None:
        query = f"UPDATE {obj.clorm_table_name} SET {self.name} = ? WHERE id = ?"
        params = (value, obj.id)
        obj.clorm.execute(query, params)
        obj._clorm_data[self.name] = value

    @property
    def typ(self) -> type[T]:
        if not hasattr(self, "_typ"):
            self._resolve_type()
        return self._typ

    @property
    def allow_none(self) -> type[T]:
        if not hasattr(self, "_typ"):
            self._resolve_type()
        return self._allow_none

    def get_type_parameter(self) -> Any:
        if hasattr(self, "__orig_class__"):
            return self.__orig_class__
        for base in self.__orig_bases__:
            if isinstance(get_origin(base), Field):
                return base
        raise TypeError("Cannot resolve generic Field class")

    def _resolve_type(self) -> None:
        param = self.get_type_parameter()
        (arg,) = get_args(param)
        if isinstance(arg, type):
            self._typ = arg
            self._allow_none = False
            return
        origin = get_origin(arg)
        if origin is Union or origin is UnionType:
            args = get_args(arg)
            if NoneType in args:
                (arg,) = (obj for obj in args if obj is not NoneType)
                if isinstance(arg, type):
                    self._typ = arg
                    self._allow_none = True
                    return
        raise TypeError(f"Unsupported type {param}")


EnumT = TypeVar("EnumT", bound=enum.Enum)


class EnumField(Field[EnumT]):
    def serialize(self, value: EnumT) -> Any:
        if self.allow_none and value is None:
            return None
        return value.value


class IdField(Field[int]):
    def __get__(self, obj: Model | None, objtype: object = None) -> int:
        if obj is None:
            return self
        return obj._clorm_data[self.name]

    def __set__(self, obj: Model, value: Never) -> None:
        raise AttributeError("Cannot set id field")


class Model:
    clorm: ClassVar[Clorm]
    clorm_table_name: ClassVar[str]

    _clorm_fields: ClassVar[dict[str, Field]]
    _clorm_instance_cache: ClassVar[weakref.WeakValueDictionary[int, Self]]
    _clorm_data: dict[int, Any]

    id = IdField()

    def __init_subclass__(cls) -> None:
        if not hasattr(cls, "clorm_table_name"):
            return  # abstract class

        cls._clorm_instance_cache = weakref.WeakValueDictionary()
        cls._clorm_fields = {}
        for obj in cls.__dict__.values():
            if isinstance(obj, Field):
                if not hasattr(obj, "name"):
                    raise RuntimeError("field does not have a name")
                cls._clorm_fields[obj.name] = obj

    def __init__(self, id: int, **kwargs: Any) -> None:
        self._clorm_data = {"id": id, **kwargs}

    def __new__(cls, id: int, **kwargs: Any) -> Self:
        if id in cls._clorm_instance_cache:
            inst = cls._clorm_instance_cache[id]
            inst._clorm_data.update(kwargs)
        else:
            inst = super().__new__(cls)
            inst.__init__(id, **kwargs)
            cls._clorm_instance_cache[id] = inst
        return inst

    def load(self) -> None:
        query = f"SELECT * FROM {self.clorm_table_name} WHERE id = ?"
        row = self.clorm.select_one(query, (self.id,))
        if row is None:
            cursor = self.clorm.conn.cursor()
            res = cursor.execute("SELECT * FROM taxon")
            print(res.fetchall())
            raise DoesNotExist(self.id)
        self._clorm_data.update(row)

    @classmethod
    def create(cls, **kwargs: Any) -> Self:
        column_names = ",".join(kwargs)
        placeholders = ",".join("?" for _ in kwargs)
        params = tuple(
            cls._clorm_fields[key].serialize(value) for key, value in kwargs.items()
        )
        query = (
            f"INSERT INTO {cls.clorm_table_name}({column_names}) VALUES({placeholders})"
        )
        cursor = cls.clorm.execute(query, params)
        return cls(cursor.lastrowid)

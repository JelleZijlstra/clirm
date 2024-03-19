from __future__ import annotations

import enum
import sqlite3
import sys
import weakref
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field, replace
from types import NoneType, UnionType
from typing import (
    Any,
    ClassVar,
    ForwardRef,
    Generic,
    Literal,
    NewType,
    Self,
    TypeVar,
    Union,
    get_args,
    get_origin,
    overload,
)

import typing_extensions

Id = NewType("Id", int)
T = TypeVar("T")


class DoesNotExist(Exception):
    """Raised when trying to access an object that does not exist."""


@dataclass
class Clorm:
    conn: sqlite3.Connection
    models: dict[str, type[Model]] = field(default_factory=dict)

    def get_name_to_model_cls(self) -> dict[str, type[Model]]:
        return {cls.__name__: cls for cls in self.models.values()}

    def select_one(
        self, query: str, parameters: tuple[Any, ...] = ()
    ) -> Mapping[str, Any] | None:
        cursor = self.conn.cursor()
        cursor.row_factory = sqlite3.Row
        res = cursor.execute(query, parameters)
        return res.fetchone()

    def select(self, query: str, parameters: tuple[Any, ...] = ()) -> sqlite3.Cursor:
        cursor = self.conn.cursor()
        cursor.row_factory = sqlite3.Row
        return cursor.execute(query, parameters)

    def select_tuple(self, query: str, parameters: tuple[Any, ...] = ()) -> Any:
        cursor = self.conn.cursor()
        cursor.row_factory = sqlite3.Row
        res = cursor.execute(query, parameters)
        return res.fetchone()

    def execute(self, query: str, parameters: tuple[Any, ...] = ()) -> sqlite3.Cursor:
        cursor = self.conn.cursor()
        res = cursor.execute(query, parameters)
        self.conn.commit()
        return res


class Condition:
    def __or__(self, other: Condition) -> OrCondition:
        return OrCondition(self, other)

    def __inv__(self) -> NotCondition:
        return NotCondition(self)

    def stringify(self) -> tuple[str, tuple[object, ...]]:
        raise NotImplementedError


@dataclass
class Comparison(Condition):
    left: Field
    operator: Literal["<", "<=", ">", ">=", "=", "!="]
    right: Any

    def stringify(self) -> tuple[str, tuple[object, ...]]:
        if self.right is None:
            match self.operator:
                case "=":
                    return f"({self.left.name} IS NULL)", ()
                case "!=":
                    return f"({self.left.name} IS NOT NULL)", ()
                case _:
                    raise TypeError("Unsupported operator")
        right = self.left.serialize(self.right)
        return f"({self.left.name} {self.operator} ?)", (right,)


@dataclass
class OrCondition(Condition):
    left: Condition
    right: Condition

    def stringify(self) -> tuple[str, tuple[object, ...]]:
        left, left_args = self.left.stringify()
        right, right_args = self.right.stringify()
        return f"({left} OR {right})", (*left_args, *right_args)


@dataclass
class NotCondition(Condition):
    cond: Condition

    def stringify(self) -> tuple[str, tuple[object, ...]]:
        query, args = self.cond.stringify()
        return f"NOT {query}", args


@dataclass
class Contains(Condition):
    left: Field
    positive: bool
    values: Sequence[Any]

    def stringify(self) -> tuple[str, tuple[object, ...]]:
        vals = tuple(self.left.serialize(val) for val in self.values)
        condition = "IN" if self.positive else "NOT IN"
        return f"({self.left.name} {condition} ?)", (vals,)


@dataclass
class OrderBy:
    field: Field
    ascending: bool

    def stringify(self) -> str:
        direction = "ASC" if self.ascending else "DESC"
        return f"{self.field.name} {direction}"


ModelT = TypeVar("ModelT", bound="Model")


@dataclass
class Query(Generic[ModelT]):
    model: type[ModelT]
    conditions: Sequence[Condition] = ()
    order_by_columns: Sequence[OrderBy] = ()
    limit_clause: int | None = None

    def filter(self, *conds: Condition) -> Query:
        return replace(self, conditions=[*self.conditions, *conds])

    def limit(self, limit: int) -> Query:
        return replace(self, limit_clause=limit)

    def order_by(self, *orders: OrderBy) -> Query:
        return replace(self, order_by_columns=[*self.order_by_columns, *orders])

    def stringify(self, columns: str = "*") -> tuple[str, tuple[object, ...]]:
        query = f"SELECT {columns} FROM {self.model.clorm_table_name}"
        params = []
        if self.conditions:
            pairs = [cond.stringify() for cond in self.conditions]
            where = " AND ".join(cond for cond, _ in pairs)
            query = f"{query} WHERE {where}"
            params += (param for _, params in pairs for param in params)
        if self.order_by_columns:
            order_by = ", ".join(item.stringify() for item in self.order_by_columns)
            query = f"{query} ORDER BY {order_by}"
        if self.limit_clause is not None:
            query += " LIMIT ?"
            params.append(self.limit_clause)
        return query, tuple(params)

    def count(self) -> int:
        query, params = self.stringify("COUNT(*)")
        (count,) = self.model.clorm.select_tuple(query, params)
        return count

    def __iter__(self) -> Iterator[ModelT]:
        query, params = self.stringify()
        cursor = self.model.clorm.select(query, params)
        while True:
            rows = cursor.fetchmany()
            if not rows:
                break
            for row in rows:
                yield self.model(**row)


class Field(Generic[T]):
    name: str
    _type_object: type[object]
    _allow_none: bool
    _full_type: Any
    model_cls: type[Model]

    def __init__(self, name: str | None = None) -> None:
        if name is not None:
            self.name = name

    def __set_name__(self, owner: object, name: str) -> None:
        if not hasattr(self, "name"):
            self.name = name

    @overload
    def __get__(self, obj: None, objtype: object = None) -> Self: ...
    @overload
    def __get__(self, obj: Model | None, objtype: object = None) -> T: ...

    def __get__(self, obj: Model | None, objtype: object = None) -> T | Self:
        if obj is None:
            return self
        raw_value = self.get_raw(obj)
        return self.deserialize(raw_value)

    def deserialize(self, raw_value: Any) -> T:
        if self.allow_none and raw_value is None:
            return None
        return self.type_object(raw_value)

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
        if not isinstance(value, self.type_object):
            raise TypeError(
                f"Cannot set value {value!r} in field of type {self.type_object}"
            )
        if issubclass(self.type_object, enum.Enum):
            return value.value
        if issubclass(self.type_object, Model):
            return value.id
        return value

    def set_raw(self, obj: Model, value: Any) -> None:
        if self.full_type is Id:
            raise AttributeError("Cannot set id field")
        query = f"UPDATE {obj.clorm_table_name} SET {self.name} = ? WHERE id = ?"
        params = (value, obj.id)
        obj.clorm.execute(query, params)
        obj._clorm_data[self.name] = value

    @property
    def type_object(self) -> type[T]:
        self._resolve_type()
        return self._type_object

    @property
    def allow_none(self) -> bool:
        self._resolve_type()
        return self._allow_none

    @property
    def full_type(self) -> Any:
        self._resolve_type()
        return self._full_type

    def get_type_parameter(self) -> Any:
        if hasattr(self, "__orig_class__"):
            return self.__orig_class__
        for base in self.__orig_bases__:
            if isinstance(get_origin(base), Field):
                return base
        raise TypeError("Cannot resolve generic Field class")

    def _resolve_type(self) -> None:
        if hasattr(self, "_type_object"):
            return
        self._full_type, self._type_object, self._allow_none = self._get_resolved_type()

    def _get_resolved_type(self) -> tuple[Any, type[object], bool]:
        param = self.get_type_parameter()
        (arg,) = get_args(param)
        if isinstance(arg, ForwardRef):
            ns = {
                **self.model_cls.clorm.get_name_to_model_cls(),
                **sys.modules[self.model_cls.__module__].__dict__,
            }
            arg = eval(arg.__forward_code__, ns)
        if isinstance(arg, type):
            return (arg, arg, False)
        if arg is Self or arg is typing_extensions.Self:
            return (self.model_cls, self.model_cls, False)
        if arg is Id:
            return (arg, int, False)
        origin = get_origin(arg)
        if origin is Union or origin is UnionType:
            args = get_args(arg)
            if NoneType in args:
                (arg,) = (obj for obj in args if obj is not NoneType)
                if isinstance(arg, type):
                    return (arg | None, arg, True)
                elif arg is Self or arg is typing_extensions.Self:
                    return (self.model_cls | None, self.model_cls, True)
        raise TypeError(f"Unsupported type {param}")

    def __eq__(self, other: T) -> Condition:
        return Comparison(self, "=", other)

    def __ne__(self, other: T) -> Condition:
        return Comparison(self, "!=", other)

    def __gt__(self, other: T) -> Condition:
        return Comparison(self, ">", other)

    def __ge__(self, other: T) -> Condition:
        return Comparison(self, ">=", other)

    def __lt__(self, other: T) -> Condition:
        return Comparison(self, "<", other)

    def __le__(self, other: T) -> Condition:
        return Comparison(self, "<=", other)

    def contains(self, other: Sequence[T]) -> Condition:
        return Contains(self, True, other)

    def asc(self) -> OrderBy:
        return OrderBy(self, True)

    def desc(self) -> OrderBy:
        return OrderBy(self, False)


MaybeModelT = TypeVar("MaybeModelT", bound="Model | None")


class ForeignKeyField(Field[MaybeModelT]):
    pass


class Model:
    clorm: ClassVar[Clorm]
    clorm_table_name: ClassVar[str]

    _clorm_fields: ClassVar[dict[str, Field]]
    _clorm_instance_cache: ClassVar[weakref.WeakValueDictionary[int, Self]]
    _clorm_has_unresolved_types: ClassVar[bool] = True
    _clorm_data: dict[str, Any]

    id = Field[Id]()

    def __init_subclass__(cls) -> None:
        if not hasattr(cls, "clorm_table_name"):
            return  # abstract class

        cls._clorm_instance_cache = weakref.WeakValueDictionary()
        cls._clorm_fields = {}
        for obj in cls.__dict__.values():
            if isinstance(obj, Field):
                if not hasattr(obj, "name"):
                    raise RuntimeError("field does not have a name")
                if hasattr(obj, "model_cls"):
                    raise RuntimeError(
                        f"field {obj.name} is already associated with a class"
                    )
                obj.model_cls = cls
                cls._clorm_fields[obj.name] = obj
        cls.clorm.models[cls.clorm_table_name] = cls

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

    @classmethod
    def select(cls) -> Query:
        return Query(cls)

    def delete_instance(self) -> None:
        query = f"DELETE FROM {self.clorm_table_name} WHERE id = ?"
        self.clorm.execute(query, (self.id,))

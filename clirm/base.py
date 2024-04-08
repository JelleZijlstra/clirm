from __future__ import annotations

import dataclasses
import enum
import sqlite3
import sys
import weakref
from collections.abc import Iterator, Mapping, Sequence
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


class UnresolvedType(Exception):
    """Raised when a field's type has not yet been resolved."""


@dataclasses.dataclass
class Clirm:
    conn: sqlite3.Connection
    models: dict[str, type[Model]] = dataclasses.field(default_factory=dict)
    models_with_unresolved_types: set[type[Model]] = dataclasses.field(
        default_factory=set
    )

    def get_name_to_model_cls(self) -> dict[str, type[Model]]:
        return {cls.__name__: cls for cls in self.models.values()}

    def try_resolve_all_types(self) -> None:
        self.models_with_unresolved_types = {
            model
            for model in self.models_with_unresolved_types
            if not model.clirm_try_resolve_types()
        }

    def select_one(
        self, query: str, parameters: tuple[Any, ...] = ()
    ) -> Mapping[str, Any] | None:
        with self.conn:
            cursor = self.conn.cursor()
            cursor.row_factory = sqlite3.Row
            res = cursor.execute(query, parameters)
            return res.fetchone()

    def select(self, query: str, parameters: tuple[Any, ...] = ()) -> sqlite3.Cursor:
        with self.conn:
            cursor = self.conn.cursor()
            cursor.row_factory = sqlite3.Row
            return cursor.execute(query, parameters)

    def select_tuple(self, query: str, parameters: tuple[Any, ...] = ()) -> Any:
        with self.conn:
            cursor = self.conn.cursor()
            res = cursor.execute(query, parameters)
            return res.fetchone()

    def execute(self, query: str, parameters: tuple[Any, ...] = ()) -> sqlite3.Cursor:
        with self.conn:
            cursor = self.conn.cursor()
            res = cursor.execute(query, parameters)
            self.conn.commit()
            return res


class Condition:
    def __or__(self, other: Condition) -> OrCondition:
        return OrCondition(self, other)

    def __invert__(self) -> NotCondition:
        return NotCondition(self)

    def stringify(self) -> tuple[str, tuple[object, ...]]:
        raise NotImplementedError


@dataclasses.dataclass
class Comparison(Condition):
    left: Field[Any]
    operator: Literal["<", "<=", ">", ">=", "=", "!=", "INSTR", "LIKE"]
    right: Any

    def stringify(self) -> tuple[str, tuple[object, ...]]:
        if self.right is None:
            match self.operator:
                case "=":
                    return f"(`{self.left.name}` IS NULL)", ()
                case "!=":
                    return f"(`{self.left.name}` IS NOT NULL)", ()
                case _:
                    raise TypeError("Unsupported operator")
        right = self.left.serialize(self.right)
        match self.operator:
            case "INSTR":
                return f"INSTR(`{self.left.name}`, ?)", (right,)
            case _:
                return f"(`{self.left.name}` {self.operator} ?)", (right,)
        assert False, "unreachable"


@dataclasses.dataclass
class OrCondition(Condition):
    left: Condition
    right: Condition

    def stringify(self) -> tuple[str, tuple[object, ...]]:
        left, left_args = self.left.stringify()
        right, right_args = self.right.stringify()
        return f"({left} OR {right})", (*left_args, *right_args)


@dataclasses.dataclass
class NotCondition(Condition):
    cond: Condition

    def stringify(self) -> tuple[str, tuple[object, ...]]:
        query, args = self.cond.stringify()
        return f"NOT {query}", args


@dataclasses.dataclass
class Contains(Condition):
    left: Field
    positive: bool
    values: Sequence[Any]

    def stringify(self) -> tuple[str, tuple[object, ...]]:
        vals = tuple(self.left.serialize(val) for val in self.values)
        placeholders = ", ".join("?" for _ in vals)
        condition = "IN" if self.positive else "NOT IN"
        return f"(`{self.left.name}` {condition} ({placeholders}))", vals


@dataclasses.dataclass
class Func:
    name: str

    def stringify(self) -> tuple[str, tuple[object, ...]]:
        return f"{self.name}()", ()


@dataclasses.dataclass
class OrderBy:
    field: Field[Any]
    ascending: bool

    def stringify(self) -> tuple[str, tuple[object, ...]]:
        direction = "ASC" if self.ascending else "DESC"
        return f"`{self.field.name}` {direction}", ()


ModelT = TypeVar("ModelT", bound="Model")


@dataclasses.dataclass
class Query(Generic[ModelT]):
    model: type[ModelT]
    conditions: Sequence[Condition] = ()
    order_by_columns: Sequence[OrderBy | Func] = ()
    limit_clause: int | None = None

    def filter(self, *conds: Condition, **kwargs: Any) -> Query[ModelT]:
        kwargs_conds = [
            getattr(self.model, key) == value for key, value in kwargs.items()
        ]
        return dataclasses.replace(
            self, conditions=[*self.conditions, *conds, *kwargs_conds]
        )

    def limit(self, limit: int | None) -> Query[ModelT]:
        return dataclasses.replace(self, limit_clause=limit)

    def order_by(self, *orders: OrderBy | Field[object] | Func) -> Query[ModelT]:
        clauses = [
            OrderBy(item, True) if isinstance(item, Field) else item for item in orders
        ]
        return dataclasses.replace(
            self, order_by_columns=[*self.order_by_columns, *clauses]
        )

    def stringify(self, columns: str = "*") -> tuple[str, tuple[object, ...]]:
        query = f"SELECT {columns} FROM {self.model.clirm_table_name}"
        params: list[object] = []
        if self.conditions:
            pairs = [cond.stringify() for cond in self.conditions]
            where = " AND ".join(cond for cond, _ in pairs)
            query = f"{query} WHERE {where}"
            params += (param for _, params in pairs for param in params)
        if self.order_by_columns:
            pairs = [item.stringify() for item in self.order_by_columns]
            order_by = ", ".join(item for item, _ in pairs)
            params += (param for _, params in pairs for param in params)
            query = f"{query} ORDER BY {order_by}"
        if self.limit_clause is not None:
            query += " LIMIT ?"
            params.append(self.limit_clause)
        return query, tuple(params)

    def count(self) -> int:
        query, params = self.stringify("COUNT(*)")
        (count,) = self.model.clirm.select_tuple(query, params)
        return count

    def get(self) -> ModelT:
        for obj in self:
            return obj
        raise DoesNotExist(self.model)

    def __iter__(self) -> Iterator[ModelT]:
        query, params = self.stringify()
        cursor = self.model.clirm.select(query, params)
        while True:
            rows = cursor.fetchmany()
            if not rows:
                break
            for row in rows:
                yield self.model(**row)


class Field(Generic[T]):
    name: str
    default: T | None
    _type_object: type[object]
    _allow_none: bool
    _full_type: Any
    model_cls: type[Model]
    related_name: str | None = None

    def __init__(
        self,
        name: str | None = None,
        *,
        default: T | None = None,
        related_name: str | None = None,
    ) -> None:
        if name is not None:
            self.name = name
        self.default = default
        self.related_name = related_name

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
        if self.name not in obj._clirm_data:
            obj.load()
        return obj._clirm_data[self.name]

    def __set__(self, obj: Model, value: T) -> None:
        raw_value = self.serialize(value)
        if self.name in obj._clirm_data and obj._clirm_data[self.name] == raw_value:
            return
        self.set_raw(obj, raw_value)

    def serialize(self, value: T) -> Any:
        if self.allow_none and value is None:
            return None
        if type(value) in (int, str):
            return value
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
        obj._clirm_data[self.name] = value
        obj._clirm_dirty_fields.add(self.name)
        obj.save()

    @property
    def type_object(self) -> type[T]:
        self.resolve_type()
        return self._type_object

    @property
    def allow_none(self) -> bool:
        self.resolve_type()
        return self._allow_none

    @property
    def full_type(self) -> Any:
        self.resolve_type()
        return self._full_type

    def get_type_parameter(self) -> Any:
        if hasattr(self, "__orig_class__"):
            return self.__orig_class__
        for base in self.__orig_bases__:
            if get_origin(base) is Field:
                return base
        raise TypeError("Cannot resolve generic Field class")

    def resolve_type(self) -> None:
        if hasattr(self, "_type_object"):
            return
        self._full_type, self._type_object, self._allow_none = self.get_resolved_type()
        if issubclass(self._type_object, Model):
            if self.related_name is None:
                self.related_name = self.model_cls.clirm_table_name + "_set"
            if hasattr(self._type_object, self.related_name):
                raise TypeError(
                    f"{self._type_object} already has an attribute {self.related_name}; cannot set related_name for {self}"
                )
            setattr(
                self._type_object, self.related_name, make_foreign_key_accessor(self)
            )
            self._type_object.clirm_backrefs.append(self)
        elif self.related_name is not None:
            raise TypeError(
                "Cannot set related_name on fields that are not foreign keys"
            )

    def resolve_forward_ref(self, arg: ForwardRef) -> Any:
        ns = {
            **self.model_cls.clirm.get_name_to_model_cls(),
            **sys.modules[self.model_cls.__module__].__dict__,
        }
        try:
            return eval(arg.__forward_code__, ns)
        except (NameError, AttributeError) as e:
            raise UnresolvedType from e

    def get_resolved_type(self) -> tuple[Any, type[object], bool]:
        param = self.get_type_parameter()
        (arg,) = get_args(param)
        if isinstance(arg, ForwardRef):
            arg = self.resolve_forward_ref(arg)
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
        return self.resolve_type_fallback(arg)

    def resolve_type_fallback(self, arg: Any) -> tuple[Any, type[object], bool]:
        raise TypeError(f"Unsupported type {arg} for field {self}")

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

    def __mod__(self: Field[str], other: str) -> Condition:
        return Comparison(self, "LIKE", other)

    def contains(self, other: T) -> Condition:
        return Comparison(self, "INSTR", other)

    def startswith(self: Field[str], substring: str) -> Condition:
        return Comparison(self, "LIKE", substring + "%")

    def endswith(self: Field[str], substring: str) -> Condition:
        return Comparison(self, "LIKE", "%" + substring)

    def is_in(self, other: Sequence[T]) -> Condition:
        return Contains(self, True, other)

    def is_not_in(self, other: Sequence[T]) -> Condition:
        return Contains(self, False, other)

    def asc(self) -> OrderBy:
        return OrderBy(self, True)

    def desc(self) -> OrderBy:
        return OrderBy(self, False)

    def __repr__(self) -> str:
        return f"<Field: {self.name}>"


def make_foreign_key_accessor(field: Field[Any]) -> Any:
    @property
    def accessor(self: Any) -> Query[field.model_cls]:
        return field.model_cls.select().filter(field == self)

    return accessor


class Model:
    # Must be set in subclasses
    clirm: ClassVar[Clirm]
    clirm_table_name: ClassVar[str]

    # Set by the abstraction
    clirm_fields: ClassVar[dict[str, Field[Any]]]
    clirm_backrefs: list[Field[Any]]
    _clirm_instance_cache: ClassVar[weakref.WeakValueDictionary[int, Self]]
    _clirm_has_unresolved_types: ClassVar[bool] = True
    _clirm_data: dict[str, Any]

    DoesNotExist = DoesNotExist

    id = Field[Id]()

    def __init_subclass__(cls) -> None:
        if not hasattr(cls, "clirm_table_name"):
            return  # abstract class

        cls._clirm_instance_cache = weakref.WeakValueDictionary()
        cls.clirm_fields = {}
        cls.clirm_backrefs = []
        for name, obj in cls.__dict__.items():
            if isinstance(obj, Field):
                if not hasattr(obj, "name"):
                    raise RuntimeError("field does not have a name")
                if hasattr(obj, "model_cls"):
                    raise RuntimeError(
                        f"field {obj.name} is already associated with a class"
                    )
                obj.model_cls = cls
                cls.clirm_fields[name] = obj
        cls.clirm.models[cls.clirm_table_name] = cls
        cls.clirm.models_with_unresolved_types.add(cls)
        cls.clirm.try_resolve_all_types()

    @classmethod
    def clirm_try_resolve_types(cls) -> bool:
        has_unresolved_types = False
        for field in cls.clirm_fields.values():
            try:
                field.resolve_type()
            except UnresolvedType:
                has_unresolved_types = True
        return not has_unresolved_types

    def __init__(self, id: int, **kwargs: Any) -> None:
        self._clirm_data = {"id": id, **kwargs}
        self._clirm_dirty_fields = set()

    def __new__(cls, id: int, **kwargs: Any) -> Self:
        if id in cls._clirm_instance_cache:
            inst = cls._clirm_instance_cache[id]
            inst._clirm_data.update(kwargs)
        else:
            inst = super().__new__(cls)
            inst.__init__(id, **kwargs)
            cls._clirm_instance_cache[id] = inst
        return inst

    def load(self) -> None:
        query = f"SELECT * FROM `{self.clirm_table_name}` WHERE id = ?"
        row = self.clirm.select_one(query, (self.id,))
        if row is None:
            raise DoesNotExist(self.id)
        self._clirm_data.update(row)

    def save(self) -> None:
        if not self._clirm_dirty_fields:
            return
        updates = ", ".join(
            f"`{field_name}` = ?" for field_name in self._clirm_dirty_fields
        )
        params = [self._clirm_data[field] for field in self._clirm_dirty_fields]
        query = f"UPDATE `{self.clirm_table_name}` SET {updates} WHERE id = ?"
        self.clirm.execute(query, (*params, self.id))
        self._clirm_dirty_fields.clear()

    def serialize(self) -> Self:
        self.load()
        return self

    @classmethod
    def create(cls, **kwargs: Any) -> Self:
        column_names: list[str] = []
        params: list[object] = []
        for name, field in cls.clirm_fields.items():
            if name in kwargs:
                cooked_value = kwargs.pop(name)
            else:
                cooked_value = field.default
                if cooked_value is None and not field.allow_none:
                    continue
            value = field.serialize(cooked_value)
            column_names.append(field.name)
            params.append(value)
        if kwargs:
            raise TypeError(f"Extra kwargs {', '.join(kwargs)}")

        placeholders = ",".join("?" for _ in column_names)
        colnames_str = ",".join(f"`{name}`" for name in column_names)
        query = (
            f"INSERT INTO {cls.clirm_table_name}({colnames_str}) VALUES({placeholders})"
        )
        cursor = cls.clirm.execute(query, tuple(params))
        assert cursor.lastrowid is not None
        return cls(cursor.lastrowid)

    @classmethod
    def select(cls) -> Query[Self]:
        return Query(cls)

    @classmethod
    def filter(cls, *conditions: Condition, **kwargs: Any) -> Query[Self]:
        return cls.select().filter(*conditions, **kwargs)

    @classmethod
    def get(cls, *conditions: Condition, **kwargs: Any) -> Self:
        return cls.select().filter(*conditions, **kwargs).get()

    def delete_instance(self) -> None:
        query = f"DELETE FROM {self.clirm_table_name} WHERE id = ?"
        self.clirm.execute(query, (self.id,))

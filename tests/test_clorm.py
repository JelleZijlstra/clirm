import enum
import json
import sqlite3
import typing
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Self, TypeVar

import pytest

from clorm import Clorm, Field, Model


class Status(enum.Enum):
    valid = 1
    nomen_dubium = 2


def make_clorm(tables: Sequence[str]) -> Clorm:
    # bug in inspect?
    conn = sqlite3.connect(":memory:")  # static analysis: ignore[internal_error]
    for table in tables:
        conn.execute(table)
    conn.commit()

    return Clorm(conn)


def test() -> None:
    clorm_global = make_clorm(
        ["CREATE TABLE taxon(id INTEGER PRIMARY KEY, name, extinct, status)"]
    )

    class Taxon(Model):
        clorm = clorm_global
        clorm_table_name = "taxon"

        name = Field[str]()
        extinct = Field[bool]()
        status = Field[Status | None]()

    txn = Taxon.create(name="Neurotrichus", extinct=False)
    assert txn.name == "Neurotrichus"
    assert txn.extinct is False
    assert txn.status is None

    txn2 = Taxon(txn.id)
    assert txn is txn2

    txn3 = Taxon.create(
        name="Scotophilus borbonicus", extinct=True, status=Status.nomen_dubium
    )
    assert txn3.name == "Scotophilus borbonicus"
    assert txn3.extinct is True
    assert txn3.status is Status.nomen_dubium

    txn3.status = Status.valid
    assert txn3.status is Status.valid

    id3 = txn3.id
    del txn3

    txn4 = Taxon(id3)
    assert txn4.status is Status.valid

    assert Taxon.select().count() == 2
    assert Taxon.select().filter(Taxon.extinct == True).count() == 1

    rows = list(Taxon.select())
    assert len(rows) == 2
    assert txn in rows
    assert txn4 in rows

    txn.delete_instance()
    assert Taxon.select().count() == 1
    txn4.delete_instance()
    assert Taxon.select().count() == 0

    for i in range(5):
        Taxon.create(name=f"Taxon{i}", extinct=i != 1)

    assert [t.name for t in Taxon.select()] == [f"Taxon{i}" for i in range(5)]
    assert [t.name for t in Taxon.select().limit(2)] == [f"Taxon{i}" for i in range(2)]
    assert [t.name for t in Taxon.select().filter(Taxon.extinct == False)] == ["Taxon1"]
    assert [t.name for t in Taxon.select().order_by(Taxon.name.desc())] == [
        f"Taxon{4 - i}" for i in range(5)
    ]


def test_foreign_key() -> None:
    # bug in inspect?
    conn = sqlite3.connect(":memory:")  # static analysis: ignore[internal_error]
    conn.execute("CREATE TABLE taxon(id INTEGER PRIMARY KEY, name, extinct, status)")
    conn.commit()

    clorm_global = make_clorm(
        [
            "CREATE TABLE taxon(id INTEGER PRIMARY KEY, parent, valid_name, base_name)",
            "CREATE TABLE name(id INTEGER PRIMARY KEY, taxon, root_name)",
        ]
    )

    class Name(Model):
        clorm = clorm_global
        clorm_table_name = "name"

        taxon = Field["Taxon"](related_name="names")
        root_name = Field[str]()

    class Taxon(Model):
        clorm = clorm_global
        clorm_table_name = "taxon"

        valid_name = Field[str]()
        parent = Field[Self | None](related_name="children")
        base_name = Field[Name | None]()

    txn = Taxon.create(valid_name="Talpidae")
    assert txn.children.count() == 0
    assert txn.names.count() == 0
    assert txn.parent is None
    assert txn.base_name is None
    txn2 = Taxon.create(valid_name="Neurotrichus", parent=txn)
    assert txn.children.count() == 1
    assert txn2.parent is txn
    assert txn2.base_name is None

    nam = Name.create(taxon=txn, root_name="Talp")
    assert nam.taxon is txn
    assert nam.root_name == "Talp"
    assert txn.names.count() == 1
    txn.base_name = nam
    assert txn.base_name is nam


def test_default() -> None:
    clorm_global = make_clorm(
        [
            "CREATE TABLE taxon(id INTEGER PRIMARY KEY, name NOT NULL, is_extinct, status)"
        ]
    )

    class Taxon(Model):
        clorm = clorm_global
        clorm_table_name = "taxon"

        name = Field[str]()
        extinct = Field[bool]("is_extinct", default=False)
        status = Field[Status | None]()

    txn = Taxon.create(name="Neurotrichus")
    assert txn.name == "Neurotrichus"
    assert txn.extinct is False
    assert txn.status is None

    txn2 = Taxon.create(name="Megalomys", extinct=True)
    assert txn2.name == "Megalomys"
    assert txn2.extinct is True
    assert txn2.status is None

    txn3 = Taxon.create(name="Veratalpa", status=Status.nomen_dubium)
    assert txn3.name == "Veratalpa"
    assert txn3.extinct is False
    assert txn3.status is Status.nomen_dubium

    with pytest.raises(sqlite3.IntegrityError):
        Taxon.create(extinct=True)  # name must be given
    with pytest.raises(TypeError):
        Taxon.create(not_a_kwarg=1, name="Veratalpa")


@dataclass
class ADT:
    value: int

    @classmethod
    def unserialize(cls, val: Any) -> Self:
        return cls(val)

    def serialize(self) -> Any:
        return self.value


ADTT = TypeVar("ADTT", bound=ADT)


class ADTField(Field[Sequence[ADTT]]):
    adt_type: type[ADT]

    def deserialize(self, raw_value: Any) -> Sequence[ADTT]:
        if isinstance(raw_value, str) and raw_value:
            if not hasattr(self, "_adt_cache"):
                self._adt_cache = {}
            if raw_value in self._adt_cache:
                return self._adt_cache[raw_value]
            tags = tuple(
                self.adt_type.unserialize(val) for val in json.loads(raw_value)
            )
            self._adt_cache[raw_value] = tags
            return tags
        else:
            return ()

    def serialize(self, value: Sequence[ADTT]) -> str | None:
        if isinstance(value, tuple):
            value = list(value)
        if isinstance(value, list):
            if value:
                return json.dumps([val.serialize() for val in value])
            else:
                return None
        elif value is None:
            return None
        raise TypeError(f"Unsupported type {value}")

    def get_resolved_type(self) -> tuple[Any, type[object], bool]:
        orig_class = self.__orig_class__
        (arg,) = typing.get_args(orig_class)
        if isinstance(arg, typing.ForwardRef):
            arg = self.resolve_forward_ref(arg)
        if not issubclass(arg, ADT):
            raise TypeError(f"ADTField must be instantiated with an ADT, not {arg}")
        self.adt_type = arg
        return (Sequence[arg], Sequence, True)


def test_adt() -> None:
    clorm_global = make_clorm(
        ["CREATE TABLE taxon(id INTEGER PRIMARY KEY, name NOT NULL, tags)"]
    )

    class Taxon(Model):
        clorm = clorm_global
        clorm_table_name = "taxon"

        name = Field[str]()
        tags = ADTField[ADT]()

    txn = Taxon.create(name="Neurotrichus")
    assert txn.tags == ()
    txn.tags = (ADT(1),)
    assert txn.tags == (ADT(1),)


def test_in_and_contains() -> None:
    clorm_global = make_clorm(
        ["CREATE TABLE taxon(id INTEGER PRIMARY KEY, name NOT NULL)"]
    )

    class Taxon(Model):
        clorm = clorm_global
        clorm_table_name = "taxon"

        name = Field[str]()

    Taxon.create(name="Neurotrichus")
    Taxon.create(name="Urotrichus")
    Taxon.create(name="Uropsilus")

    assert {
        txn.name for txn in Taxon.select().filter(Taxon.name.contains("trichus"))
    } == {"Urotrichus", "Neurotrichus"}
    assert {
        txn.name for txn in Taxon.select().filter(~Taxon.name.contains("trichus"))
    } == {"Uropsilus"}
    assert {
        txn.name for txn in Taxon.select().filter(Taxon.name.is_in(["trichus"]))
    } == set()
    assert {
        txn.name
        for txn in Taxon.select().filter(
            Taxon.name.is_in(["Urotrichus", "Uropsilus", "Talpa"])
        )
    } == {"Urotrichus", "Uropsilus"}

import enum
import json
import sqlite3
import typing
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Self, TypeVar

import pytest

from clirm import Clirm, Field, Model, ReadOnlyError, readonly


class Status(enum.Enum):
    valid = 1
    nomen_dubium = 2


def make_clirm(tables: Sequence[str]) -> Clirm:
    # bug in inspect?
    conn = sqlite3.connect(":memory:")  # static analysis: ignore[internal_error]
    for table in tables:
        conn.execute(table)
    conn.commit()

    return Clirm(conn)


def test() -> None:
    clirm_global = make_clirm(
        ["CREATE TABLE taxon(id INTEGER PRIMARY KEY, name, extinct, status)"]
    )

    class Taxon(Model):
        clirm = clirm_global
        clirm_table_name = "taxon"

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

    clirm_global = make_clirm(
        [
            "CREATE TABLE taxon(id INTEGER PRIMARY KEY, parent, valid_name, base_name)",
            "CREATE TABLE name(id INTEGER PRIMARY KEY, taxon, root_name)",
        ]
    )

    class Name(Model):
        clirm = clirm_global
        clirm_table_name = "name"

        taxon = Field["Taxon"](related_name="names")
        root_name = Field[str]()

    class Taxon(Model):
        clirm = clirm_global
        clirm_table_name = "taxon"

        valid_name = Field[str]()
        parent = Field[Self | None](related_name="children")
        base_name = Field[Name | None]()

    txn = Taxon.create(valid_name="Talpidae")
    assert txn.children.count() == 0  # static analysis: ignore[undefined_attribute]
    assert txn.names.count() == 0  # static analysis: ignore[undefined_attribute]
    assert txn.parent is None
    assert txn.base_name is None
    txn2 = Taxon.create(valid_name="Neurotrichus", parent=txn)
    assert txn.children.count() == 1  # static analysis: ignore[undefined_attribute]
    assert txn2.parent is txn
    assert txn2.base_name is None

    nam = Name.create(taxon=txn, root_name="Talp")
    assert nam.taxon is txn
    assert nam.root_name == "Talp"
    assert txn.names.count() == 1  # static analysis: ignore[undefined_attribute]
    txn.base_name = nam
    assert txn.base_name is nam


def test_default() -> None:
    clirm_global = make_clirm(
        [
            "CREATE TABLE taxon(id INTEGER PRIMARY KEY, name NOT NULL, is_extinct, status)"
        ]
    )

    class Taxon(Model):
        clirm = clirm_global
        clirm_table_name = "taxon"

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
    clirm_global = make_clirm(
        ["CREATE TABLE taxon(id INTEGER PRIMARY KEY, name NOT NULL, tags)"]
    )

    class Taxon(Model):
        clirm = clirm_global
        clirm_table_name = "taxon"

        name = Field[str]()
        tags = ADTField[ADT]()

    txn = Taxon.create(name="Neurotrichus")
    assert txn.tags == ()
    txn.tags = (ADT(1),)
    assert txn.tags == (ADT(1),)


def test_in_and_contains() -> None:
    clirm_global = make_clirm(
        ["CREATE TABLE taxon(id INTEGER PRIMARY KEY, name NOT NULL)"]
    )

    class Taxon(Model):
        clirm = clirm_global
        clirm_table_name = "taxon"

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


def test_database_readonly() -> None:
    clirm_global = make_clirm(
        ["CREATE TABLE taxon(id INTEGER PRIMARY KEY, name NOT NULL)"]
    )

    class Taxon(Model):
        clirm = clirm_global
        clirm_table_name = "taxon"

        name = Field[str]()

    txn = Taxon.create(name="Neurotrichus")
    with clirm_global.readonly():
        assert clirm_global.is_read_only
        assert Taxon.get(Taxon.name == "Neurotrichus") is txn

        with pytest.raises(ReadOnlyError):
            Taxon.create(name="Urotrichus")
        with pytest.raises(ReadOnlyError):
            txn.name = "Urotrichus"
        assert txn.name == "Neurotrichus"
        with pytest.raises(ReadOnlyError):
            txn.delete_instance()
        with pytest.raises(ReadOnlyError):
            clirm_global.execute("DELETE FROM taxon")
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            clirm_global.conn.execute("DELETE FROM taxon")

        with clirm_global.readonly():
            assert clirm_global.is_read_only
        assert clirm_global.is_read_only

    assert not clirm_global.is_read_only
    txn.name = "Urotrichus"
    assert txn.name == "Urotrichus"


def test_global_readonly() -> None:
    first_clirm = make_clirm(
        ["CREATE TABLE first_model(id INTEGER PRIMARY KEY, name NOT NULL)"]
    )
    second_clirm = make_clirm(
        ["CREATE TABLE second_model(id INTEGER PRIMARY KEY, name NOT NULL)"]
    )

    class FirstModel(Model):
        clirm = first_clirm
        clirm_table_name = "first_model"

        name = Field[str]()

    class SecondModel(Model):
        clirm = second_clirm
        clirm_table_name = "second_model"

        name = Field[str]()

    with readonly():
        assert first_clirm.is_read_only
        assert second_clirm.is_read_only
        with pytest.raises(ReadOnlyError):
            FirstModel.create(name="first")
        with pytest.raises(ReadOnlyError):
            SecondModel.create(name="second")

        third_clirm = make_clirm(
            ["CREATE TABLE third_model(id INTEGER PRIMARY KEY, name NOT NULL)"]
        )

        class ThirdModel(Model):
            clirm = third_clirm
            clirm_table_name = "third_model"

            name = Field[str]()

        assert third_clirm.is_read_only
        with pytest.raises(ReadOnlyError):
            ThirdModel.create(name="third")

    assert not first_clirm.is_read_only
    assert not second_clirm.is_read_only
    assert not third_clirm.is_read_only
    FirstModel.create(name="first")
    SecondModel.create(name="second")
    ThirdModel.create(name="third")


def test_database_readonly_does_not_affect_other_databases() -> None:
    first_clirm = make_clirm(
        ["CREATE TABLE first_model(id INTEGER PRIMARY KEY, name NOT NULL)"]
    )
    second_clirm = make_clirm(
        ["CREATE TABLE second_model(id INTEGER PRIMARY KEY, name NOT NULL)"]
    )

    class FirstModel(Model):
        clirm = first_clirm
        clirm_table_name = "first_model"

        name = Field[str]()

    class SecondModel(Model):
        clirm = second_clirm
        clirm_table_name = "second_model"

        name = Field[str]()

    with first_clirm.readonly():
        with pytest.raises(ReadOnlyError):
            FirstModel.create(name="first")
        SecondModel.create(name="second")


def test_database_readonly_restores_query_only() -> None:
    conn = sqlite3.connect(":memory:")  # static analysis: ignore[internal_error]
    conn.execute("PRAGMA query_only = ON")
    clirm_global = Clirm(conn)

    with clirm_global.readonly():
        assert conn.execute("PRAGMA query_only").fetchone() == (1,)

    assert conn.execute("PRAGMA query_only").fetchone() == (1,)


@pytest.mark.parametrize("value", ["1", "true", "yes", "ON", "enabled"])
def test_readonly_environment_variable(
    monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    monkeypatch.setenv("CLIRM_READONLY", value)
    clirm_global = make_clirm(
        ["CREATE TABLE taxon(id INTEGER PRIMARY KEY, name NOT NULL)"]
    )

    class Taxon(Model):
        clirm = clirm_global
        clirm_table_name = "taxon"

        name = Field[str]()

    assert clirm_global.is_read_only
    with pytest.raises(ReadOnlyError):
        Taxon.create(name="Neurotrichus")


@pytest.mark.parametrize("value", ["", "0", "false", "NO", "off", " false "])
def test_false_readonly_environment_variable(
    monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    monkeypatch.setenv("CLIRM_READONLY", value)
    clirm_global = make_clirm(
        ["CREATE TABLE taxon(id INTEGER PRIMARY KEY, name NOT NULL)"]
    )

    class Taxon(Model):
        clirm = clirm_global
        clirm_table_name = "taxon"

        name = Field[str]()

    assert not clirm_global.is_read_only
    Taxon.create(name="Neurotrichus")

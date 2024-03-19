import enum
import sqlite3
from collections.abc import Sequence
from typing import Self

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

        taxon = Field["Taxon"]()
        root_name = Field[str]()

    class Taxon(Model):
        clorm = clorm_global
        clorm_table_name = "taxon"

        valid_name = Field[str]()
        parent = Field[Self | None]()
        base_name = Field[Name | None]()

    txn = Taxon.create(valid_name="Talpidae")
    assert txn.parent is None
    assert txn.base_name is None
    txn2 = Taxon.create(valid_name="Neurotrichus", parent=txn)
    assert txn2.parent is txn
    assert txn2.base_name is None

    nam = Name.create(taxon=txn, root_name="Talp")
    assert nam.taxon is txn
    assert nam.root_name == "Talp"
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

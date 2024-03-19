import enum
import sqlite3

from clorm import Clorm, EnumField, Field, Model


class Status(enum.Enum):
    valid = 1
    nomen_dubium = 2


def test() -> None:
    # bug in inspect?
    conn = sqlite3.connect(":memory:")  # static analysis: ignore[internal_error]
    conn.execute("CREATE TABLE taxon(id INTEGER PRIMARY KEY, name, extinct, status)")
    conn.commit()

    clorm_global = Clorm(conn)

    class Taxon(Model):
        clorm = clorm_global
        clorm_table_name = "taxon"

        name = Field[str]()
        extinct = Field[bool]()
        status = EnumField[Status | None]()

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

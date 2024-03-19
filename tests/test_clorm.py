from clorm import Clorm, Field, Model, EnumField
import enum
import sqlite3


class Status(enum.Enum):
    valid = 1
    nomen_dubium = 2


def test() -> None:
    conn = sqlite3.connect(":memory:")
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

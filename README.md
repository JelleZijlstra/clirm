# clirm

Command-Line ORM (clirm) is a library for creating simple ORMs that
can be used in command-line programs that allow users to manipulate
objects in an interactive context, and that also regularly run scripts
that iterate over the entire database.

Features include:

- Changes are always committed to the database immediately, so that
  there is no need to worry about a separate later "save" step.
- There is always only one object per database row, so that users
  do not need to worry about editing one copy and leaving another
  ORM object corresponding to the same row unchanged.
- All columns in a row are always fetched together, to simplify
  implementation of the above point.
- Tight integration with the type system. Fields can be declared
  as `Field[T]()`, where `T` is a normal Python type.

Clirm requires that every table has an `id` column containing
a unique identifier.

## Usage

As an example, we will create a simple database containing
animal taxa:

```python
import enum
import sqlite3
from typing import Self

from clirm import Clirm, Field, Model

class Status(enum.Enum):
    living = 1
    recently_extinct = 2
    fossil = 3

CLIRM = Clirm(sqlite3.connect("taxon.db"))

class Taxon(Model):
    clirm = CLIRM
    clirm_table_name = "taxon"

    name = Field[str]()  # string field
    status = Field[Status]()  # enum field
    common_name = Field[str | None]()  # nullable string field
    parent = Field[Self | None]()  # foreign key to self; can also write "Taxon | None"

if __name__ == "__main__":
    txn1 = Taxon.create(
        name="Mammalia", status=Status.living, common_name="Mammals"
    )
    txn2 = Taxon.create(
        name="Rodentia", status=Status.living, common_name="Rodents",
        parent=txn1
    )
    tnx3 = Taxon.create(
        name="Multituberculata", status=Status.fossil, parent=txn1
    )

    living_taxa = Taxon.select().filter(Taxon.status == Status.living)
    assert living_taxa.count() == 2
    assert {txn.common_name for txn in living_taxa} == {"Mammals", "Rodents"}
    for txn in living_taxa:
        txn.common_name = txn.common_name + "!"

    # Change is immediately visible
    assert txn1.common_name == "Mammals!"
```

## Supported types

The following field types are currently supported:

- Primitive types, e.g., `int`, `str`, `bool`, which are passed
  directly to the database
- Enums, which are converted to their value before being passed to
  the database
- Foreign keys to other clirm models, which are stored as their
  IDs
- Foreign keys to the current class, which can be expressed with `typing.Self`
- Nullable versions of any of the above, expressed by adding `| None`
  to the type

Additional types can be supported by subclassing `Field` and overriding
the `deserialize` and `serialize` methods.

## Backends

For now only SQLite is supported as a backend.

## Changelog

Version 0.1 (April 8, 2024)

- Initial release

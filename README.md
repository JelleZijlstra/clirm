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

## Read-only mode

Set `CLIRM_READONLY=1` to make all `Clirm` databases created by the process
read-only. The values `0`, `false`, `no`, and `off` (case-insensitive), as well
as an unset or empty variable, leave writes enabled; any other nonempty value
enables read-only mode.

Read-only mode can also be enabled temporarily in code, either for every
database or for a single `Clirm` instance:

```python
from clirm import ReadOnlyError, readonly

with readonly():
    # Writes through any Clirm instance raise ReadOnlyError.
    inspect_all_databases()

with CLIRM.readonly():
    # Only writes to CLIRM are disabled.
    inspect_taxa()
```

Both context managers are nestable. Reads continue to work. SQLite's
`query_only` mode is enabled for the connection while a read-only context is
active, so writes attempted directly through `Clirm.conn` are also rejected.
The original `query_only` setting is restored on exit.

## Virtual models

Models may also be created as mutable, in-memory objects without inserting a row:

```python
virtual_taxon = Taxon.virtual(name="Lagomorpha", status=Status.living)
virtual_taxon.common_name = "Rabbits and relatives"

persisted_taxon = Taxon.get(name="Rodentia")
proposal = persisted_taxon.virtual_copy(common_name="Rodents and allies")
assert proposal.virtual_origin is persisted_taxon
assert proposal.virtual_origin_id == persisted_taxon.id
assert proposal.virtual_changes() == {"common_name": "Rodents and allies"}
```

Every virtual model has a unique, process-local negative `VirtualId`, including a
virtual copy of an existing row. Virtual models do not enter the database-backed
instance cache. Nullable fields and fields with defaults are populated automatically;
reading any other omitted field raises `UnsetVirtualFieldError`.

Assignments to virtual models change only their in-memory values, including inside a
read-only context. Calling `load()`, `save()`, or `delete_instance()` on a virtual model
raises `VirtualModelError`. Persisted models and `Model.create()` reject direct foreign
keys to virtual models with `VirtualReferenceError`. There is intentionally no method
to commit a virtual model to the database.

Code that validates a proposed object can temporarily substitute virtual copies for
their persisted origins when following ORM references:

```python
with substitute_virtual_models([proposal]):
    assert proposal.taxon.base_name is proposal
    assert list(NameUsage.filter(NameUsage.name == proposal))
```

Within this context, foreign keys to an origin resolve to its virtual copy, and query
predicates involving that copy use the origin's database ID. This is a reference
overlay, not a virtual database: newly created virtual objects are not added to
queries, and predicates on changed scalar fields still query persisted data.

## Backends

For now only SQLite is supported as a backend.

## Changelog

Version 0.3.1 (August 3, 2026)

- Fix version

Version 0.3.0 (August 3, 2026)

- Add support for substituting virtual models

Version 0.2.0 (August 3, 2026)

- Add support for virtual mode
- Add support for read-only mode

Version 0.1 (April 8, 2024)

- Initial release

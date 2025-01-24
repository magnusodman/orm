import json
import sqlite3
from pathlib import Path
import types
from typing import Self, get_origin

from pydantic import BaseModel
from pydantic.fields import FieldInfo

type TableId = int


class TableModel(BaseModel):
    id: TableId = None

    def __init__(self, **data):
        super().__init__(**data)

    @classmethod
    def find_by_id(cls, id: TableId) -> Self:
        table_name = cls.__name__
        con = cls.__connection__()
        cur = con.cursor()
        fields = ", ".join(cls.model_fields.keys())
        cur.execute(f"SELECT {fields} FROM {table_name} WHERE id = {id}")
        row = cur.fetchone()
        if row is None:
            return None
        return cls(
            **{k[0]: cls.__unmarshal(k, v) for k, v in zip(cur.description, row)}
        )

    @classmethod
    def __unmarshal(cls, k, v):
        field_name = k[0]
        field = cls.model_fields[field_name]
        origin = get_origin(field.annotation) or field.annotation
        if origin is list:
            if type(field.annotation) is types.GenericAlias:
                # check if list is of type str, int, float, bool, bytes
                return json.loads(v)
            else:
                return v
        return v

    def save(self):
        table_name = self.__class__.__name__
        # Check if table exists in __orm_db__["tables"]
        if (
            globals().get("__orm_db__", {}).get("tables", {}).get(table_name, None)
            is None
        ):
            # Create table if it does not exist
            table = self.__create_table(table_name)
            if "tables" not in globals()["__orm_db__"]:
                globals()["__orm_db__"]["tables"] = {}
            globals()["__orm_db__"]["tables"][table_name] = {}
        if self.id is None:
            # Insert new record
            self.__insert_record(table_name)
        else:
            # Update existing record
            self.__update_record(table_name)
        return self

    def __getattr(self, field_name: str, field: FieldInfo):
        # check for json field
        tp = field.annotation
        origin = get_origin(tp) or tp
        if origin is list:
            if type(tp) is types.GenericAlias:
                # check if list is of type str, int, float, bool, bytes
                return json.dumps(getattr(self, field_name))
            else:
                return getattr(self, field_name)

        return getattr(self, field_name)

    def __insert_record(self, table_name):
        con = self.__connection__()
        cur = con.cursor()
        fields = ", ".join([k for k in self.model_fields.keys() if k != "id"])
        values = ", ".join(
            [
                f"'{self.__getattr(field_name, field)}'"
                for field_name, field in self.model_fields.items()
                if field_name != "id"
            ]
        )
        cur.execute(f"INSERT INTO {table_name} ({fields}) VALUES ({values})")
        con.commit()
        self.id = cur.lastrowid

    def __field_name(self, field_name: str, field: FieldInfo):
        return field_name if field.alias is None else field.alias

    def __field_type(self, field: FieldInfo):
        tp = field.annotation
        origin = get_origin(tp) or tp
        if origin is str:
            return "TEXT"
        elif origin is int:
            return "INTEGER"
        elif origin is float:
            return "REAL"
        elif origin is bool:
            return "BOOLEAN"
        elif origin is bytes:
            return "BLOB"
        elif origin is list:
            if type(tp) is types.GenericAlias:
                # check if list is of type str, int, float, bool, bytes
                if tp.__args__[0] in [str, int, float, bool, bytes]:
                    return "TEXT"

            pass
        else:
            raise ValueError(f"Type {tp} not supported")

    def field_nullable(self, field: FieldInfo):
        return "NOT NULL" if field.is_required() else "NULL"

    def __create_table(self, table_name):
        con = self.__connection__()

        cur = con.cursor()
        # Iterate over fields and create columns
        cur.execute(f"CREATE TABLE {table_name} (id INTEGER PRIMARY KEY AUTOINCREMENT)")
        for field_name, field in self.model_fields.items():
            if field.annotation is TableId:
                continue
            cur.execute(
                f"ALTER TABLE {table_name} ADD COLUMN {self.__field_name(field_name, field)} {self.__field_type(field)} {self.field_nullable(field)}"
            )
        con.commit()

    @classmethod
    def __connection__(self):
        # check if __orm_db__ is defined globally
        if "__orm_db__" in globals() and "connection" in globals()["__orm_db__"]:
            return globals()["__orm_db__"]["connection"]
        else:
            # create connection and store it globally
            con = sqlite3.connect("orm.sqlite")
            if "__orm_db__" not in globals():
                globals()["__orm_db__"] = {}
            globals()["__orm_db__"]["connection"] = con
            return con


class User(TableModel):
    name: str
    age: int
    tags: list[str] = []
    seq: list[int] = []


def main():
    user = User(name="John", age=25, tags=["1", "2", "3"], seq=[3, 2, 1]).save()
    print(user)
    user = User.find_by_id(1)
    print(user)


if __name__ == "__main__":
    Path("orm.sqlite").unlink(missing_ok=True)
    main()

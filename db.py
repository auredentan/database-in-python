import enum
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass
class DbInfo:
    tables: list[str]


DB_INFO_FILE = Path("db_info.json")
DB_INFO: DbInfo


class MetaCommandResult(enum.Enum):
    META_COMMAND_SUCCESS = 1
    META_COMMAND_UNRECOGNIZED_COMMAND = 2


class PrepareResult(enum.Enum):
    PREPARE_SUCCESS = 1
    PREPARE_UNRECOGNIZED_STATEMENT = 2


class StatementType(enum.Enum):
    STATEMENT_INSERT = 1
    STATEMENT_SELECT = 2
    STATEMENT_CREATE = 3


class CreateType(enum.Enum):
    TABLE = 1


@dataclass(kw_only=True)
class Statement:
    type: StatementType
    cmd: str


@dataclass
class Column:
    name: str
    type: str


@dataclass(kw_only=True)
class CreateTableCmd:
    name: str
    columns: list[Column]


@dataclass
class TableMetadata:
    columns: list[Column]


def save_db_info() -> None:
    with open(DB_INFO_FILE, "w") as fp:
        json.dump(asdict(DB_INFO), fp)


def print_tables() -> None:
    if not DB_INFO.tables:
        print("No tables found")
    for t in DB_INFO.tables:
        print(f"Table: {t}\n")


def print_table_info(table_name: str, metadata: TableMetadata) -> None:
    print(f"Table: {table_name}\n------------")
    for col in metadata.columns:
        print(f"{col.name}: {col.type}")


def execute_d_cmd(cmd: str) -> None:
    args = cmd.split(" ")
    if len(args) == 1:
        print_tables()
        return

    table_name = args[1]
    table_metadata = read_table_metadata(table_name)
    print_table_info(table_name, table_metadata)


def do_meta_command(cmd: str) -> MetaCommandResult:
    if cmd == ".exit":
        sys.exit(0)
    if cmd.startswith(".d"):
        execute_d_cmd(cmd)
        return MetaCommandResult.META_COMMAND_SUCCESS
    return MetaCommandResult.META_COMMAND_UNRECOGNIZED_COMMAND


def prepare_statement(cmd: str) -> tuple[PrepareResult, Statement | None]:
    if cmd[:6].lower() == "create":
        return (
            PrepareResult.PREPARE_SUCCESS,
            Statement(type=StatementType.STATEMENT_CREATE, cmd=cmd),
        )
    if cmd[:6].lower() == "insert":
        return (
            PrepareResult.PREPARE_SUCCESS,
            Statement(type=StatementType.STATEMENT_INSERT, cmd=cmd),
        )
    if cmd[:6].lower() == "select":
        return (
            PrepareResult.PREPARE_SUCCESS,
            Statement(type=StatementType.STATEMENT_SELECT, cmd=cmd),
        )
    return (PrepareResult.PREPARE_UNRECOGNIZED_STATEMENT, None)


def parse_columns(cmd: str) -> list[Column]:
    cols = cmd.removeprefix("(").removesuffix(")").split(",")
    columns: list[Column] = []
    for col in cols:
        splitted_col = col.strip().split(" ")
        columns.append(Column(name=splitted_col[0], type=splitted_col[1]))

    return columns


def create_table(create_cmd: CreateTableCmd) -> None:
    table_meta = TableMetadata(columns=create_cmd.columns)
    print(f"Creating table: {table_meta}")
    with open(create_cmd.name, "w") as fp:
        json.dump(
            {
                "metadata": json.dumps(asdict(table_meta)),
                "rows": [],
            },
            fp,
        )
    DB_INFO.tables.append(create_cmd.name)


def read_table_metadata(table_name: str) -> TableMetadata:
    with open(table_name, "r") as fp:
        data = json.load(fp)
    metadata = json.loads(data["metadata"])
    return TableMetadata(
        columns=[Column(name=m["name"], type=m["type"]) for m in metadata["columns"]]
    )


def execute_create(cmd: str) -> None:
    splitted_cmd = cmd.strip().split(" ")
    if not splitted_cmd:
        print("Unknown create cmd")
        sys.exit(0)

    create_type = splitted_cmd[0].lower()

    if create_type == "table":
        create_cmd = CreateTableCmd(
            name=splitted_cmd[1], columns=parse_columns(" ".join(splitted_cmd[2:]))
        )
        create_table(create_cmd)
    elif create_type == "index":
        pass
    else:
        print(f"Unrecognized Create cmd: {create_type}")
        sys.exit(0)


def execute_statement(statement: Statement) -> None:
    if statement.type == StatementType.STATEMENT_CREATE:
        execute_create(statement.cmd[6:])


def init_db_info() -> None:
    global DB_INFO

    if not DB_INFO_FILE.exists():
        DB_INFO = DbInfo(tables=[])
        return

    with open(DB_INFO_FILE, "r") as fp:
        data = json.load(fp)
    DB_INFO = DbInfo(tables=data["tables"])


def main() -> None:
    init_db_info()
    while True:
        cmd = input("db > ")

        # Command parsing
        if cmd[0] == ".":
            cmd_result = do_meta_command(cmd)
            match cmd_result:
                case MetaCommandResult.META_COMMAND_SUCCESS:
                    continue
                case MetaCommandResult.META_COMMAND_UNRECOGNIZED_COMMAND:
                    print(f"Unrecognized command {cmd}.\n")
                    continue

        # Statement
        (prepare_result, statement) = prepare_statement(cmd)

        match prepare_result:
            case PrepareResult.PREPARE_SUCCESS:
                pass
            case PrepareResult.PREPARE_UNRECOGNIZED_STATEMENT:
                print(f"Unrecognized keyword at start of {cmd}.\n")
                continue

        assert statement is not None
        execute_statement(statement)
        print("Executed.\n")
        save_db_info()


if __name__ == "__main__":
    main()

import enum
import sys
from dataclasses import dataclass


class MetaCommandResult(enum.Enum):
    META_COMMAND_SUCCESS = 1
    META_COMMAND_UNRECOGNIZED_COMMAND = 2


def do_meta_command(cmd: str) -> MetaCommandResult:
    if cmd == ".exit":
        sys.exit(0)
    return MetaCommandResult.META_COMMAND_UNRECOGNIZED_COMMAND


class PrepareResult(enum.Enum):
    PREPARE_SUCCESS = 1
    PREPARE_UNRECOGNIZED_STATEMENT = 2


class StatementType(enum.Enum):
    STATEMENT_INSERT = 1
    STATEMENT_SELECT = 2


@dataclass
class Statement:
    type: StatementType


def prepare_statement(
    cmd: str
) -> tuple[PrepareResult, Statement | None]:
    if cmd[:6].lower() == "insert":
        return (
            PrepareResult.PREPARE_SUCCESS,
            Statement(type=StatementType.STATEMENT_INSERT),
        )
    if cmd[:6].lower() == "select":
        return (
            PrepareResult.PREPARE_SUCCESS,
            Statement(type=StatementType.STATEMENT_SELECT),
        )
    return (PrepareResult.PREPARE_UNRECOGNIZED_STATEMENT, None)


def execute_statement(statement: Statement) -> None:
    pass


def main() -> None:
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


if __name__ == "__main__":
    main()

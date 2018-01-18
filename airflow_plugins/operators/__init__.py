from .base import BashOperator, ExecutableOperator, FileOperator
from .csv import CSVSQL, CSVLook, CSVStats, CSVtoDB, DBtoCSV, SplitCSVtoDB
from .db import (
    ChangeDatabaseName,
    CreateDatabase,
    CreateTableWithColumns,
    DropDatabase,
    PostgresOperator
)
from .defer import DeferOperator
from .files import (
    DeleteFile,
    DownloadFile,
    DynamicDeleteFile,
    DynamicDownloadFile,
    DynamicUploadFile,
    UploadFile
)
from .run_evaluation import RunEvaluationOperator
from .sensors import FileSensor, FTPDirSensor, TaskRuntimeSensor
from .slack import Message, SlackMessageSensor
from .zip import UnzipOperator, ZipOperator

OPERATORS = [
    BashOperator, ChangeDatabaseName, CreateDatabase,
    CreateTableWithColumns, CSVLook, CSVSQL, CSVStats, CSVtoDB, DBtoCSV,
    DeferOperator, DeleteFile, DownloadFile,
    DropDatabase, DynamicDeleteFile, DynamicDownloadFile, DynamicUploadFile,
    ExecutableOperator, FileOperator, FileSensor, FTPDirSensor,
    Message, PostgresOperator,
    RunEvaluationOperator,
    SlackMessageSensor, SplitCSVtoDB,
    TaskRuntimeSensor, UnzipOperator, UploadFile, ZipOperator,
]

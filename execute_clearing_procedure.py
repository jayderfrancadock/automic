#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script com a função de executar uma determinada procedure no banco de dados do emissor
e registrar a execução no banco de dados do logbatch.
"""

__version__ = "1.0.0"

from sys import argv as _argv, stderr as _stderr
from argparse import ArgumentParser, RawDescriptionHelpFormatter, Action
from textwrap import dedent
from datetime import datetime, timedelta
from timeit import default_timer as timer
import traceback
import pymssql

# constantes de retorno
SUCCESS_CODE = 0
# erros gerais do python
PARAMETER_ERROR_CODE = 100
# erros de banco de dados
DATABASE_ERROR_CODE = 200
PROCEDURE_ERROR_CODE = 201

class CustomArgumentParser(ArgumentParser):
    def error(self, message):
        self.print_usage(_stderr)
        self.exit(PARAMETER_ERROR_CODE, '%s: error: %s\n' % (self.prog, message))

class ValidateEmptyStringAction(Action):
    def __init__(self, option_strings, dest, nargs=None, **kwargs):
        if nargs is not None:
            raise ValueError("nargs not allowed")
        super().__init__(option_strings, dest, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        if values.strip() == "":
            parser.error(f"{option_string} cannot be empty.")
        setattr(namespace, self.dest, values)

def _log(message):
    print(message)

def log_with_datetime(message):
    _log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {message}")

def log_info(message):
    log_with_datetime(f"[INFO] {message}")

def log_error(message):
    log_with_datetime(f"[ERROR] {message}")

def log_warn(message):
    log_with_datetime(f"[WARN] {message}")

def parse_arguments(args):
    version_text = dedent(f"%(prog)s {__version__}")
    description_text = dedent(__doc__)

    parser = CustomArgumentParser(description=description_text, formatter_class=RawDescriptionHelpFormatter, add_help=False)

    params = parser.add_argument_group()
    params.add_argument("--issuer-server",
                        required=True, action=ValidateEmptyStringAction,
                        help="servidor do banco de dados do emissor")
    params.add_argument("--issuer-database",
                        required=True, action=ValidateEmptyStringAction,
                        help="banco de dados do emissor")
    params.add_argument("--issuer-user",
                        required=True, action=ValidateEmptyStringAction,
                        help="usuario do banco de dados do emissor")
    params.add_argument("--issuer-password",
                        required=True, action=ValidateEmptyStringAction,
                        help="senha do usuario do banco de dados do emissor")

    params.add_argument("--logbatch-server",
                        required=True, action=ValidateEmptyStringAction,
                        help="servidor do banco de dados para registro da execucao")
    params.add_argument("--logbatch-database",
                        required=True, action=ValidateEmptyStringAction,
                        help="banco de dados para registro da execucao")
    params.add_argument("--logbatch-user",
                        required=True, action=ValidateEmptyStringAction,
                        help="usuario do banco de dados para registro da execucao")
    params.add_argument("--logbatch-password",
                        required=True, action=ValidateEmptyStringAction,
                        help="senha do usuario do banco de dados para registro da execucao")

    params.add_argument("--issuer-procedure",
                        required=True, action=ValidateEmptyStringAction,
                        help="nome da procedure que sera executada no banco do emissor")

    optionals = parser.add_argument_group()
    optionals.add_argument("-v", "--version",
                           action='version', version=version_text, help="output version information and exit")
    optionals.add_argument("-h", "--help",
                           action='help', help="display this help and exit")

    return parser.parse_args(args)

def db_connect(server, database, user, password, autocommit=True):
    return pymssql.connect(
        server=server,
        user=user,
        password=password,
        database=database,
        as_dict=True,
        autocommit=autocommit
    )

def register_new_proc_execution(conn_params, server, database, procedure):
    with db_connect(**conn_params) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"INSERT INTO dbo.HistoricoAutomicClearingLog "
                    "(DataExecucao, Emissor, Ip_servidor, Nome_Procedure, "
                    "DataHoraInicial, DataHoraFinal, MensagemErro, StatusExecucao) "
                f"VALUES (CAST(SYSDATETIME() AS DATE), '{database}', '{server}', '{procedure}', "
                          "SYSDATETIME(), null, null, 1)"
            )
            return cursor.lastrowid

def update_proc_execution_success(conn_params, rowid):
    with db_connect(**conn_params) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"UPDATE dbo.HistoricoAutomicClearingLog "
                "SET DataHoraFinal = SYSDATETIME(), StatusExecucao = 2"
                f"WHERE HistoricoAutomicClearingLog = {rowid}"
            )
            affected = cursor.rowcount
            return affected

def update_proc_execution_error(conn_params, rowid, error):
    with db_connect(**conn_params) as conn:
        with conn.cursor() as cursor:
            error_message = str(error).replace("'", "''")
            cursor.execute(
                f"UPDATE dbo.HistoricoAutomicClearingLog "
                f"SET DataHoraFinal = SYSDATETIME(), MensagemErro = '{error_message}', StatusExecucao = 3 "
                f"WHERE HistoricoAutomicClearingLog = {rowid}"
            )
            affected = cursor.rowcount
            return affected

def check_procedure_exists(conn_params, procedure):
    try:
        with db_connect(**conn_params) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT * "
                    "FROM sys.objects "
                    f"WHERE object_id = OBJECT_ID(N'dbo.{procedure}') "
                    "AND type in (N'P', N'PC')"
                )
                rows = cursor.fetchall()
                if len(rows) > 0:
                    return None
                else:
                    return Exception(f"Procedure '{procedure}' not found or user does not have permission to execute.")
    except pymssql.Error as error:
        return error

def callback_msghandler(msgstate, severity, srvname, procname, line, msgtext):
    if procname is not None or procname.strip() != "":
        log_warn(f"Server {srvname}, Level {severity}, State {msgstate}, Procedure {procname}, Line {line}: {msgtext}")
    else:
        log_warn(f"Server {srvname}, Level {severity}, State {msgstate}, Line {line}: {msgtext}")

def execute_procedure(conn_params, procedure):

    try:
        with db_connect(**conn_params) as conn:

            # pego a conexao nativa
            # para obter a exception real,
            # pois somente nela que tem o nome da procedure
            # que gerou erro
            _mssql_conn = conn._conn

            # captura de mensagens do console SQL
            _mssql_conn.set_msghandler(callback_msghandler)

            # executa a procedure informada sem parametros
            _mssql_conn.execute_non_query(
                "SET NOCOUNT ON; "
                "SET DEADLOCK_PRIORITY HIGH; "
                f"EXEC [dbo].[{procedure}]; "
            )

            # execucao com sucesso nao retorna nenhum objeto com erro
            return None

    except pymssql.Error as error:
        return error

    except pymssql._mssql.MSSQLException as error:

        number = error.number
        procname = error.procname
        severity = error.severity
        state = error.state
        line = error.line
        text = error.text

        msg_tuple = error.args[0]
        class_ex = None

        # adiciona uma nova entrada na tupla com mais informacoes
        if procname is not None:
            msg_tuple += ('SQL Server message %d, severity %d, state %d, procedure %s, line %d' %
                         (number, severity, state, procname, line)),
        else:
            msg_tuple += ('SQL Server message %d, severity %d, state %d, line %d' %
                         (number, severity, state, line)),

        if type(error) is pymssql._mssql.MSSQLDatabaseException:
            # determina o tipo de erro conforme a biblioteca
            # https://github.com/pymssql/pymssql/blob/fd2baefa4e185f5a47e5bb99cc2fc6b2c7cb6e42/src/pymssql/_pymssql.pyx#L57
            # https://github.com/pymssql/pymssql/blob/fd2baefa4e185f5a47e5bb99cc2fc6b2c7cb6e42/src/pymssql/_pymssql.pyx#L460
            if number in (102, 207, 208, 2812, 4104):
                class_ex = pymssql.ProgrammingError
            elif number in (515, 547, 2601, 2627):
                class_ex = pymssql.IntegrityError
            else:
                class_ex = pymssql.OperationalError
        elif type(error) is pymssql._mssql.MSSQLDriverException:
            class_ex = pymssql.InternalError
        else:
            class_ex = pymssql.DatabaseError

        # nao eh um erro lancado pelo pymssql
        return class_ex(msg_tuple)

def run_process(params):

    log_info("Processo de execucao de procedure ... ")
    _log("")

    log_info(f"Procedure a ser executada: {params.issuer_procedure}")

    _log("")
    log_info("Realizando conexao com banco de dados")
    log_info(f"Emissor  - Instancia: {params.issuer_server}")
    log_info(f"Emissor  - Banco de dados: {params.issuer_database}")
    log_info(f"Emissor  - Usuario: {params.issuer_user}")
    _log("")
    log_info(f"LogBatch - Instancia: {params.logbatch_server}")
    log_info(f"LogBatch - Banco de dados: {params.logbatch_database}")
    log_info(f"LogBatch - Usuario: {params.logbatch_user}")
    _log("")

    issuer_conn_params = {
        "server": params.issuer_server,
        "database": params.issuer_database,
        "user": params.issuer_user,
        "password": params.issuer_password
    }

    logbatch_conn_params = {
        "server": params.logbatch_server,
        "database": params.logbatch_database,
        "user": params.logbatch_user,
        "password": params.logbatch_password
    }

    try:

        log_info(f"Registrando nova execucao da procedure ...")
        rowid = register_new_proc_execution(logbatch_conn_params, params.issuer_server,
                                            params.issuer_database, params.issuer_procedure)
        log_info(f"Id da execucao registrada '{rowid}' na HistoricoAutomicClearingLog")

        log_info(f"Verficando a existencia da procedure no banco de dados ...")
        error = check_procedure_exists(issuer_conn_params, params.issuer_procedure)

        if error is not None:
            update_proc_execution_error(logbatch_conn_params, rowid, error)
            log_error(f"Erro ao validar a procedure {params.issuer_procedure}")
            log_error(str(error))
            _log("")
            _log(''.join(traceback.format_exception(error)))
            return PROCEDURE_ERROR_CODE

        log_info(f"Executando procedure ...")
        error = execute_procedure(issuer_conn_params, params.issuer_procedure)

        if error is not None:
            update_proc_execution_error(logbatch_conn_params, rowid, error)
            log_error(f"Erro ao executar a procedure {params.issuer_procedure}")
            log_error(str(error))
            _log("")
            _log(''.join(traceback.format_exception(error)))
            return PROCEDURE_ERROR_CODE

        update_proc_execution_success(logbatch_conn_params, rowid)
        log_info("Procedure executada com sucesso")

        return SUCCESS_CODE

    except pymssql.Error as error:
        log_error("Erro ao comunicar com banco de dados")
        log_error(str(error))
        _log("")
        _log(''.join(traceback.format_exception(error)))
        return DATABASE_ERROR_CODE

# metodo principal do script
def main(argv):

    # registra o inicio da execucao para calculo do tempo
    start_time = timer()

    # captura os parametros informados
    params = parse_arguments(argv)

    # executa o processo
    code = run_process(params)

    # registra o fim da execucao para calculo do tempo
    end_time = timer()

    _log("")
    _log(f"Return code: {code}")
    _log(f"Elapsed time: {str(timedelta(seconds=(end_time - start_time)))}")
    exit(code)

# entrypoint do script
if __name__ == '__main__':
    main(_argv[1:])
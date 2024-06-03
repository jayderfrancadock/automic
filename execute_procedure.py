#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Executa uma procedure com as seguintes acoes:
- Verifica se a mesma foi executada para aquela data de referencia na tabela de controle
- Verifica se a mesma esta desativada do batch ou desativada do batch para executar
- Registra a executao da mesma na tabela de controle e em caso de falha atualiza este registro
"""

__version__ = "1.0.0"
__author__ = "Jayder França <jayder.franca@dock.tech>"

import sys
from sys import argv as _argv
from argparse import ArgumentParser, RawDescriptionHelpFormatter
from textwrap import dedent
from datetime import datetime, timedelta
from timeit import default_timer as timer
import traceback
import pymssql

SUCCESS_CODE = 0

PROC_CADASTRADA_SERVICO_BATCH_CODE = 100
PROC_JA_EXECUTADA_DATAMOVIMENTO_CODE = 101

PARAM_RUN_DATE_INVALID_CODE = 201
PARAM_REQUIRED_EMPTY_CODE = 202
DATABASE_ERROR_CODE = 203
PROCEDURE_NOT_EXISTS_CODE = 204


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
    version_text = dedent(f"%(prog)s {__version__}\nAuthor: {__author__}")

    parser = ArgumentParser(description=dedent(__doc__), formatter_class=RawDescriptionHelpFormatter, add_help=False)

    params = parser.add_argument_group()
    params.add_argument("--issuer-server",
                        required=True, help="servidor do banco de dados do emissor")
    params.add_argument("--issuer-database",
                        required=True, help="banco de dados do emissor")
    params.add_argument("--issuer-user",
                        required=True, help="usuario do banco de dados do emissor")
    params.add_argument("--issuer-password",
                        required=True, help="senha do usuario do banco de dados do emissor")

    params.add_argument("--logbatch-server",
                        required=True, help="servidor do banco de dados para registro da execucao")
    params.add_argument("--logbatch-database",
                        required=True, help="banco de dados para registro da execucao")
    params.add_argument("--logbatch-user",
                        required=True, help="usuario do banco de dados para registro da execucao")
    params.add_argument("--logbatch-password",
                        required=True, help="senha do usuario do banco de dados para registro da execucao")

    params.add_argument("--issuer-procedure",
                        required=True, help="nome da procedure que sera executada no banco do emissor")

    optionals = parser.add_argument_group()
    optionals.add_argument("--remove-from-batch",
                           action="store_true", help="remove a procedure da tabela do servico batch")
    optionals.add_argument("--force-execution",
                           action="store_true", help="forca a execucao da procedure, mesmo que exista um registro do tipo 1")
    optionals.add_argument("--run-date",
                           help="data de referencia para execucao (YYYY-MM-DD)")
    optionals.add_argument("-v", "--version",
                           action='version', version=version_text, help="output version information and exit")
    optionals.add_argument("-h", "--help",
                           action='help', help="display this help and exit")

    return parser.parse_args(args)


def validate_params(params):
    if params.issuer_server.strip() == "":
        log_error("Parametro --issuer-server com valor vazio")
        return PARAM_REQUIRED_EMPTY_CODE

    if params.issuer_database.strip() == "":
        log_error("Parametro --issuer-database com valor vazio")
        return PARAM_REQUIRED_EMPTY_CODE

    if params.issuer_user.strip() == "":
        log_error("Parametro --issuer-user com valor vazio")
        return PARAM_REQUIRED_EMPTY_CODE

    if params.logbatch_server.strip() == "":
        log_error("Parametro --logbatch-server com valor vazio")
        return PARAM_REQUIRED_EMPTY_CODE

    if params.logbatch_database.strip() == "":
        log_error("Parametro --logbatch-database com valor vazio")
        return PARAM_REQUIRED_EMPTY_CODE

    if params.logbatch_user.strip() == "":
        log_error("Parametro --logbatch-user com valor vazio")
        return PARAM_REQUIRED_EMPTY_CODE

    if params.issuer_procedure.strip() == "":
        log_error("Parametro --issuer-procedure com valor vazio")
        return PARAM_REQUIRED_EMPTY_CODE

    if params.run_date != "" and params.run_date is not None:
        try:
            run_date = datetime.strptime(params.run_date, '%Y-%m-%d')
        except ValueError as error:
            log_error(f"Data de referencia informada eh invalida '{params.run_date}'!")
            traceback.print_exc()
            return PARAM_RUN_DATE_INVALID_CODE

    return SUCCESS_CODE


def db_connect(server, database, user, password):
    return pymssql.connect(
        server=server,
        user=user,
        password=password,
        database=database,
        as_dict=True
    )


def check_procedure_exists(conn, procedure):
    with conn.cursor() as cursor:
        cursor.execute(
            f"SELECT * "
            f"FROM sys.objects "
            f"WHERE object_id = OBJECT_ID(N'dbo.{procedure}') "
            f"AND type in (N'P', N'PC')"
        )
        rows = cursor.fetchall()
        if len(rows) > 0:
            return True
        else:
            return False


def check_procedure_in_batch_service(conn, procedure):
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT * "
            "FROM dbo.ProcessosProcedures WITH (NOLOCK) "
            f"WHERE NomeProcedure = '{procedure}'"
        )
        rows = cursor.fetchall()
        if len(rows) > 0:
            return True
        else:
            return False


def remove_procedure_from_batch_service(conn, procedure):

    affected = 0

    # exclusao do servico batch a procedure
    with conn.cursor() as cursor:
        cursor.execute(f"""                      
            DECLARE @ProcedureOrdem INT,
                    @Affected INT
                       
            SELECT @ProcedureOrdem = Ordem
            FROM dbo.ProcessosProcedures WITH (NOLOCK)
            WHERE NomeProcedure = '{procedure}'

            DELETE
            FROM dbo.ProcessosProcedures
            WHERE NomeProcedure = '{procedure}'
                       
            SELECT @Affected = @@ROWCOUNT
                   
            IF (@Affected > 0)
            BEGIN
                UPDATE ProcessosProcedures
                SET Ordem = Ordem - 1
                WHERE Ordem >= @ProcedureOrdem
            END
                       
            SELECT @Affected AS Affected
        """)
        row = cursor.fetchone()
        conn.commit()
        affected = row["Affected"]

    return affected


def check_procedure_already_executed(conn, procedure, server, database, run_date, force):
    with conn.cursor() as cursor:
        query = (
            "SELECT * "
            "FROM dbo.HistoricoAutomicLog WITH (NOLOCK) "
            f"WHERE Nome_Procedure = '{procedure}' "
            f"AND Ip_servidor = '{server}' "
            f"AND Emissor = '{database}' "
            f"AND DataMovimento = '{run_date}' "
        )

        # forca a execucao da procedure somente se estiver no status 1
        # status 2 a procedure foi executada com sucesso e nao pode
        # ser executada novamente
        if force:
            query += "AND StatusExecucao = 2"
        else:
            query += "AND StatusExecucao IN (1, 2)"

        cursor.execute(query)
        rows = cursor.fetchall()

        if len(rows) > 0:
            return True
        else:
            return False


def register_new_proc_execution(conn, server, database, procedure, run_date):
    with conn.cursor() as cursor:
        cursor.execute(
            f"INSERT INTO dbo.HistoricoAutomicLog "
                "(DataMovimento, Emissor, Ip_servidor, Nome_Procedure, "
                "DataHoraInicial, DataHoraFinal, MensagemErro, StatusExecucao) "
            f"VALUES ('{run_date} 00:00:00', '{database}', '{server}', '{procedure}', SYSDATETIME(), null, null, 1)"
        )
        conn.commit()
        return cursor.lastrowid


def update_proc_execution_error(conn, rowid, error):
    with conn.cursor() as cursor:
        error_message = str(error).replace("'", "''")
        cursor.execute(
             f"UPDATE dbo.HistoricoAutomicLog "
            "SET DataHoraFinal = SYSDATETIME(), "
                f"MensagemErro = '{error_message}', "
                "StatusExecucao = 3 "
            f"WHERE HistoricoAutomicLog = {rowid}"
        )
        affected = cursor.rowcount
        conn.commit()
        return affected


def update_proc_execution_success(conn, rowid):
    with conn.cursor() as cursor:
        cursor.execute(
            f"UPDATE dbo.HistoricoAutomicLog "
            "SET DataHoraFinal = SYSDATETIME(), "
                "StatusExecucao = 2"
            f"WHERE HistoricoAutomicLog = {rowid}"
        )
        affected = cursor.rowcount
        conn.commit()
        return affected


def execute_procedure(conn, procedure, run_date):
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                f"SET DEADLOCK_PRIORITY HIGH; "
                f"EXEC [dbo].[{procedure}] '{run_date}', null, null, null;"
            )
        conn.commit()
        return None
    except pymssql.Error as error:
        conn.rollback()
        return error


def run_process(params):

    log_info("Processo de execucao de procedure ... ")
    _log("")

    log_info(f"Procedure a ser executada: {params.issuer_procedure}")
    log_info(f"Data de referencia definida para: '{params.run_date}'")

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

    try:
        with (db_connect(params.issuer_server, params.issuer_database,
                         params.issuer_user, params.issuer_password) as issuer_conn,
              db_connect(params.logbatch_server, params.logbatch_database,
                         params.logbatch_user, params.logbatch_password) as logbatch_conn):

            # desativa o autocommit para maior controle
            issuer_conn.autocommit(False)
            logbatch_conn.autocommit(False)

            # procedure existe no banco do emissor ou esta acessivel ?
            log_info(f"Verficando a existencia da procedure no banco de dados ...")
            if not check_procedure_exists(issuer_conn, params.issuer_procedure):
                log_error(f"Procedure '{params.issuer_procedure}' "
                          f"nao encontrada no banco de dados '{params.issuer_database}'")
                return PROCEDURE_NOT_EXISTS_CODE

            # procedure continua cadastrada no Servico Batch ?
            log_info(f"Verificando procedure no Servico Batch ...")
            exists = check_procedure_in_batch_service(issuer_conn, params.issuer_procedure)

            if exists and params.remove_from_batch:
                # neste caso remove a procedure do Servico Batch
                log_info(f"Removendo procedure '{params.issuer_procedure}' "
                         "do servico Batch (--remove-from-batch)")
                remove_procedure_from_batch_service(issuer_conn, params.issuer_procedure)
            elif exists:
                log_error(f"Procedure '{params.issuer_procedure}' "
                          "registrada para ser executada no Servico Batch (ProcessosProcedures)")
                return PROC_CADASTRADA_SERVICO_BATCH_CODE
            else:
                log_info(f"Procedure '{params.issuer_procedure}' nao registrada no Servico Batch (ProcessosProcedures)")

            # procedure ja executou na data de referencia no dia atual ?
            log_info(f"Verificando a execucao da procedure na data de referenca ...")
            if check_procedure_already_executed(logbatch_conn, params.issuer_procedure, params.issuer_server,
                                                params.issuer_database, params.run_date, params.force_execution):
                log_error(f"Procedure '{params.issuer_procedure}' na "
                          f"data de referencia {params.run_date} já possui execucao (HistoricoAutomicLog)'")
                return PROC_JA_EXECUTADA_DATAMOVIMENTO_CODE

            log_info(f"Registrando nova execucao da procedure na data de referencia {params.run_date}")
            rowid = register_new_proc_execution(logbatch_conn, params.issuer_server, params.issuer_database,
                                                params.issuer_procedure, params.run_date)
            log_info(f"Id da execucao registrada '{rowid}' na HistoricoAutomicLog")

            log_info(f"Executando procedure com data de referencia {params.run_date}")
            error = execute_procedure(issuer_conn, params.issuer_procedure, params.run_date)

            if error is not None:
                update_proc_execution_error(logbatch_conn, rowid, error)
                log_error("Erro ao executar a procedure")
                log_error(str(error))
                _log("")
                _log(''.join(traceback.format_exception(error)))
                return DATABASE_ERROR_CODE

            update_proc_execution_success(logbatch_conn, rowid)
            log_info("Procedure executada com sucesso")

        return SUCCESS_CODE

    except pymssql.Error as error:
        log_error("Erro ao comunicar com banco de dados")
        log_error(str(error))
        _log("")
        _log(''.join(traceback.format_exception(error)))
        return DATABASE_ERROR_CODE


def main(argv):

    start_time = timer()

    params = parse_arguments(argv)

    code = validate_params(params)
    if code == 0:

        # definicao da data de movimento
        # se nao informado eh definido d-1
        if params.run_date is None or params.run_date == "":
            run_date = datetime.today() - timedelta(days=1)
            params.run_date = run_date.strftime('%Y-%m-%d')

        code = run_process(params)

    end_time = timer()

    _log("")
    _log(f"Return Code: {code}")
    _log(f"Execution Time: {str(timedelta(seconds=(end_time - start_time)))}")
    exit(code)


if __name__ == '__main__':
    main(_argv[1:])

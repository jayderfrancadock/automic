#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Realiza as seguintes validações
- Batch foi executado para determinado emissor
- O dia em questão é feriado ?
- O dia em questão é final de semana ?
"""

__version__ = "1.0.0"
__author__ = "Jayder França <jayder.franca@dock.tech>"

from sys import argv as _argv
from argparse import ArgumentParser, RawDescriptionHelpFormatter
from textwrap import dedent
from datetime import datetime, timedelta
from timeit import default_timer as timer
import traceback
import pymssql

SUCCESS_CODE = 0

BATCH_NAO_EXECUTADO_CODE = 100
BATCH_FIM_DE_SEMANA_CODE = 101
BATCH_FERIADO_CODE = 102
BATCH_DATAMOVIMENTO_INEXISTENTE_CODE = 103
BATCH_TIPOMOVIMENTO_INDETERMINADO_CODE = 104

PARAM_RUN_DATE_INVALID_CODE = 201
PARAM_REQUIRED_EMPTY_CODE = 202
DATABASE_ERROR_CODE = 203


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
    params.add_argument("--issuer-server", required=True,
                        help="ip do servidor do banco de dados para validar o batch")
    params.add_argument("--issuer-database", required=True,
                        help="banco de dados do emissor que esta tabela de validacao do batch")
    params.add_argument("--issuer-user", required=True,
                        help="usuario do banco de dados")
    params.add_argument("--issuer-password", required=True,
                        help="senha do usuario do banco de dados")

    optionals = parser.add_argument_group()
    optionals.add_argument("--run-date",
                           help="data de referencia no qual será avaliado o batch (YYYY-MM-DD)")
    optionals.add_argument("--wait-batch-complemento",
                           action="store_true", help="aguarda a finalizacao do batch de complemento")
    optionals.add_argument("-v", "--version", action='version', version=version_text,
                           help="output version information and exit")
    optionals.add_argument("-h", "--help", action='help', help="display this help and exit")

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


def run_process(params):

    try:
        log_info("Processo de validacao do BATCH ... ")
        _log("")

        log_info(f"Data de referencia definida para: '{params.run_date}'")

        _log("")
        log_info("Realizando conexao com banco de dados")
        log_info(f"Instancia: {params.issuer_server}")
        log_info(f"Banco de dados: {params.issuer_database}")
        log_info(f"Usuario: {params.issuer_user}")
        _log("")

        with (db_connect(params.issuer_server, params.issuer_database,
                         params.issuer_user, params.issuer_password) as conn,
              conn.cursor() as cursor):

            log_info(f"Obtem dados na tabela dbo.ControleProcessos com DataMovimento {params.run_date}")
            cursor.execute("SELECT DataTerminoProcessos, TipoMovimento "
                           "FROM dbo.ControleProcessos WITH (NOLOCK)"
                           f"WHERE DataMovimento = '{params.run_date}'")

            row = cursor.fetchone()

            if row is None:
                log_error("Nao foi encontrado o registro da data de referencia na tabela dbo.ControleProcessos")
                return BATCH_DATAMOVIMENTO_INEXISTENTE_CODE

            # coleta as informacoes para validacao
            data_termino = row["DataTerminoProcessos"]
            tipo_movimento = row["TipoMovimento"]

            if tipo_movimento is None or tipo_movimento not in [0, 1, 2]:
                log_error("Nao foi possivel determinar o tipo de movimento ("
                          f"'{tipo_movimento}') para a data de referencia '{params.run_date}'")
                return BATCH_TIPOMOVIMENTO_INDETERMINADO_CODE

            if tipo_movimento == 0:
                log_warn(f"Data de referencia {params.run_date} identificada como FINAL DE SEMANA")
                return BATCH_FIM_DE_SEMANA_CODE

            if tipo_movimento == 2:
                log_warn(f"Data de referencia {params.run_date} identificada como FERIADO")
                return BATCH_FERIADO_CODE

            if tipo_movimento == 1 and data_termino is None:
                log_error(f"Batch ainda nao executado para a data de referencia {params.run_date}")
                return BATCH_NAO_EXECUTADO_CODE

            log_info(f"Batch core finalizado no horario '{data_termino}'")

            if params.wait_batch_complemento:
                _log("")
                log_info(f"Obtem dados na tabela dbo.ControleProcessosProcedures com DataMovimento {params.run_date}")

                cursor.execute("SELECT cpp.Id_Processo, pp.NomeProcedure, cpp.FlagExecutado "
                               "FROM dbo.ControleProcessosProcedures cpp WITH (NOLOCK) "
                                    "LEFT JOIN dbo.ProcessosProcedures pp WITH (NOLOCK) "
                                        "ON (pp.Id_Processo = cpp.Id_Processo) "
                               f"WHERE DataMovimento = '{params.run_date}' "
                               "AND FlagExecutado IN (0, 1)")
                rows = cursor.fetchall()
                if rows is not None and len(rows) > 0:
                    log_error("Batch complemento ainda em execucao ou com erro ... ")
                    for row in rows:
                        log_error(f"Procedure '{row["NomeProcedure"]}' ({row["Id_Processo"]}) com status '{row["FlagExecutado"]}'")
                    return BATCH_NAO_EXECUTADO_CODE

                log_info(f"Batch complemento finalizado.")

        return SUCCESS_CODE

    # except pyodbc.Error as error:
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

from __future__ import annotations
import pandas as pd
from os.path import exists, join
from os import makedirs
import csv


INPUT_FILES_FOLDER = "app/input"
OUTPUT_FILES_FOLDER = "app/output"


def make_output_dir(output_folder: str) -> None:
    """
    Cria uma pasta de output se a mesma ainda não existir.
    Depende da constante OUTPUT_FILES_FOLDER estar definida

    Args:
        output_folder (str): nome da pasta de output a ser criada.
    """
    if not exists(output_folder):
        makedirs(output_folder)


def evaluate_data(
    origin_data_csv: str = "origem-dados.csv",
    types_csv: str = "tipos.csv",
    output_csv: str = "dados_finais.csv",
) -> str:
    """
    Faz o tratamendo dos csvs seguindo os requisitos do desafio.

    Requisitos do desafio:
    - dos dados carregados do arquivo (origem-dados.csv), filtre apenas os dados identificados como "CRÍTICO" na coluna status;
    - ordene o resultado filtrado pelo campo created_at;
    - inclua um novo campo "nome_tipo" que deverá ser preenchido baseado nos dados carregados do arquivo tipos.csv;

    Argumentos:
        origin_data_csv (str): nome do csv de origem, por padrão origem-dados.csv.
        types_csv (str): nome do csv de tipos, por padrão tipos.csv.
        types_csv (str): nome do csv de saída, por padrão dados_finais.csv.

    Retorno:
        str: caminho relativo definindo local aonde o arquivo de saída foi salvo.
    """
    input_origin_data = join(INPUT_FILES_FOLDER, origin_data_csv)
    types_data = join(INPUT_FILES_FOLDER, types_csv)
    df1 = pd.read_csv(input_origin_data)
    df2 = pd.read_csv(types_data).rename(columns={"id": "tipo"})
    df3 = pd.merge(df1, df2, on="tipo", how="outer")
    df3["created_at"] = pd.to_datetime(df3["created_at"])
    df3.query("status == 'CRITICO'", inplace=True)
    df3.sort_values(by="created_at", ascending=False, inplace=True)
    df3 = df3.rename(columns={"nome": "nome_tipo"})

    csv_output_file = join(OUTPUT_FILES_FOLDER, output_csv)
    df3.to_csv(csv_output_file, index=False)
    return csv_output_file


def generate_sql_output(
    csv_output_file: str, sql_output_file_name: str = "insert-dados.sql"
) -> None:
    """
    Gera o arquivo de sql a partir do csv gerado pela função evaluate_data.

    Argumentos:
        csv_output_file (str): caminho do csv de saída, que é o retorno da função evaluate_data.
        sql_output_file_name (str): nome do sql de saída, por padrão insert-dados.sql.
    """
    with open(csv_output_file, "r", encoding="utf-8") as csv_file:
        csv_reader = csv.DictReader(csv_file)
        header_row = csv_reader.fieldnames
        output_filepath = join(OUTPUT_FILES_FOLDER, sql_output_file_name)

        f = open(output_filepath, "a")

        for row in csv_reader:
            f.write("INSERT INTO dados_finais")
            f.write(f'({", ".join(header_row)})')
            f.write(" VALUES")
            line = [f"'{row[key]}'" for key in header_row]
            f.write(f'({", ".join(line)});\n')

        f.close()


def main() -> None:
    try:
        make_output_dir(OUTPUT_FILES_FOLDER)
        csv_output_file = evaluate_data()
        generate_sql_output(csv_output_file)
    except Exception as e:
        print(e)  # implementar logger


if __name__ == "__main__":
    main()

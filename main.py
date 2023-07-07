import pandas as pd
import csv
from zipfile import ZipFile


def extract_data_from_zip(zip_file: str = 'dados.zip') -> None:
    '''
    Extrai todos os dados do arquivo zip.

    Argumentos:
        zip_file (str): o caminho + nome do arquivo do arquivo zip a ser descompactado. 
    '''
    z = ZipFile(zip_file, 'r')
    z.extractall()
    z.close()


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
        output_csv (str): nome do csv de saída, por padrão dados_finais.csv.

    Retorno:
        str: caminho relativo definindo local aonde o arquivo de saída foi salvo.
    """
    df1 = pd.read_csv(origin_data_csv)
    df2 = pd.read_csv(types_csv).rename(columns={"id": "tipo"})
    df3 = pd.merge(df1, df2, on="tipo", how="outer")
    df3["created_at"] = pd.to_datetime(df3["created_at"])
    df3.query("status == 'CRITICO'", inplace=True)
    df3.sort_values(by="created_at", ascending=False, inplace=True)
    df3 = df3.rename(columns={"nome": "nome_tipo"})

    df3.to_csv(output_csv, index=False)
    return output_csv


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
        csv_reader = list(csv_reader)

        f = open(sql_output_file_name, "w")
        f.write("INSERT INTO dados_finais")
        f.write(f'({", ".join(header_row)})')
        f.write(" VALUES ")
        
        csv_reader_length = len(csv_reader)
        for i, row in enumerate(csv_reader):
            line = [f"'{row[key]}'" for key in header_row]
            if (i+1 == csv_reader_length): f.write(f'({", ".join(line)})\n')
            else: f.write(f'({", ".join(line)}),\n')
            
            
        f.write(";")
        f.close()


def main() -> None:
    try:
        extract_data_from_zip()
        csv_output_file = evaluate_data()
        generate_sql_output(csv_output_file)
    except Exception as e:
        print(e)  # implementar logger


if __name__ == "__main__":
    main()

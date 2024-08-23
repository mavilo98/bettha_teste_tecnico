import pandas as pd
import logging
from datetime import *
from schema import schemas
import os
import re


class PipelineHerois():

    def __init__(self) -> None:
        self.tables = ['heroes', 'missions', 'skills', 'super_powers', 'villains']
        self.schema_analytics = schemas
        self.data_folder = 'data'

    # Função para integrar dois arquivos
    def integrar_dados(self, camada: str, *args) -> dict:

        landing_integrada = {}

        for table in self.tables:
            files = [f'{self.data_folder}/{camada}/{x}/{x}_{table}.csv' for x in args]

            try:
                logging.info(f"Carregando dados de {table} da camama {camada}.")
                logging.info(f"Arquivos a serem processados: {files}.")
                files = [pd.read_csv(file) for file in files]

                logging.info(f"Integrando os dados de {table}.")
                df_merged = pd.concat(files, ignore_index=True)

                landing_integrada[table] = df_merged

            except FileNotFoundError as e:
                logging.error(f"Erro ao tentar carregar os arquivos: {e}")
                raise

            except pd.errors.EmptyDataError as e:
                logging.error(f"Erro: Um dos arquivos está vazio: {e}")
                raise

            except Exception as e:
                logging.error(f"Ocorreu um erro inesperado ao integrar os dados: {e}")
                raise

        return landing_integrada


    # Função para remover duplicatas
    def deduplicar_dados(self, data: dict, campos: dict) -> dict:

        for x in campos.keys():

            try:
                logging.info("Removendo duplicatas dos dados de {x}.")
                df_deduplicado = data[x].drop_duplicates(subset=campos[x])
                data[x] = df_deduplicado

            except KeyError as e:
                logging.error(f"Erro: Coluna(s) não encontrada(s) para remoção de duplicatas: {e}")
                raise

            except Exception as e:
                logging.error(f"Ocorreu um erro inesperado ao remover duplicatas: {e}")
                raise
        return data


    # Função para salvar os dados transformados na camada RAW
    def salvar_dados(self, data: dict, camada: str) -> None:

        for table in data.keys():
            df = data[table].copy()

            try:
                df['_ts_carga_raw'] = datetime.now()
                caminho_arquivo = f"{self.data_folder}/{camada}/{camada}_{table}.csv"
                
                logging.info(f"Salvando os dados transformados de {table} em {caminho_arquivo}.")
                df.to_csv(caminho_arquivo, sep = ';', decimal=',', index=False, encoding='utf-8')
                
            except IOError as e:
                logging.error(f"Erro ao tentar salvar o arquivo: {e}")
                raise

            except Exception as e:
                logging.error(f"Ocorreu um erro inesperado ao salvar os dados na camada RAW: {e}")
                raise
        return


    # Função para adequar schema e salvar em parquet
    def adequar_schema_e_salva_na_trusted(self, source_path: str, final_path: str) -> None:

        files = os.listdir(f'{self.data_folder}/{source_path}') 
        schema = self.schema_analytics

        for file in files:
            arquivo_csv = f"{self.data_folder}/{source_path}/{file}"
            tabela = re.search(r'[^_]*_(.*)\.csv', file).group(1)

            # Lê a tabela CSV
            df = pd.read_csv(arquivo_csv, sep=';', decimal=',', encoding='utf-8')

            schema_tabela = schema[tabela]

            try:
                # Converte para o schema definido
                for coluna, tipo in schema_tabela.items():
                    if coluna == 'tipo_carga':
                        continue
                    if tipo == 'Integer':
                        df[coluna] = pd.to_numeric(df[coluna], errors='coerce').fillna(0).astype(int)
                    elif tipo == 'String':
                        df[coluna] = df[coluna].astype(str)
                    elif tipo == 'Date':
                        df[coluna] = pd.to_datetime(df[coluna], errors='coerce')
                        df[coluna] = df[coluna].fillna(pd.NaT)  # Substitui valores inválidos por NaT
                    elif tipo == 'Timestamp':
                        df[coluna] = pd.to_datetime(df[coluna], errors='coerce')
                    elif tipo == 'Boolean':
                        df[coluna] = df[coluna].astype(bool)
                    else:
                        logging.warning(f"Tipo de dado não reconhecido para a coluna {coluna}.")
            
            except Exception as e:
                logging.error(f"Erro ao adequar a coluna {coluna} do arquivo {file}: {e}")
                raise
            
            arquivo_parquet = f"{self.data_folder}/{final_path}/{final_path}_{tabela}.parquet"
            
            df_final = df

            # Verifica o tipo de carga

            # Se arquivo tipo fato, caso já exista dados, concatena tabelas
            if schema_tabela['tipo_carga'] == 'fato' and os.path.exists(arquivo_parquet):
                # Realiza merge com o arquivo existente
                df_existing = pd.read_parquet(arquivo_parquet)
                df_final = pd.concat([df_existing, df], ignore_index=True).drop_duplicates()
                
            try:
                # Se o arquivo for dimensão, será sobrescrito
                df_final.to_parquet(arquivo_parquet, index=False)
                logging.info(f"Tabela {tabela} salva como {schema_tabela['tipo_carga']} em {arquivo_parquet}.")                

            except Exception as e:
                logging.error(f"Erro ao salvar a tabela {tabela}: {e}")
                raise

    # Função geral para salvar tabelas sem transformações na camada Analytics
    def salva_na_analytics(self, camada_origem, camada_destino, *args) -> None:

        for tabela in args:

            df = pd.read_parquet(f"{self.data_folder}/{camada_origem}/{camada_origem}_{tabela}.parquet")
            df.to_parquet(f"{self.data_folder}/{camada_destino}/{camada_destino}_dim_{tabela}.parquet", index=False)
            logging.info(f"Tabela dimensão {tabela} salva em parquet, sem transformações, com sucesso na camada analytics.")


    def transformar_e_salvar_fact_missions(self):

        try:
            df_missions = pd.read_parquet(f"{self.data_folder}/trusted/trusted_missions.parquet")
            
            # Calcular a duração da missão
            df_missions['duration'] = (pd.to_datetime(df_missions['end_date']) - pd.to_datetime(df_missions['start_date'])).dt.days
            
            # Adicionar um KPI de missão bem sucedida
            df_missions['success'] = df_missions['is_ongoing'] == False

            # Salvar a tabela fato
            df_missions.to_parquet(f"{self.data_folder}/analytics/analytics_fact_missions.parquet", index=False)
            logging.info("Tabela fact_missions salva em parquet com sucesso na camada analytics.")
        except Exception as e:
            logging.error(f"Erro ao tentar realizar transformações na tabela de missões: {e}")
            raise


    def transformar_e_salvar_dim_heroes(self):
        try:
            df_heroes = pd.read_parquet(f"{self.data_folder}/trusted/trusted_heroes.parquet")
            df_missions = pd.read_parquet(f"{self.data_folder}/trusted/trusted_missions.parquet")
            
            # Agregação de missões por herói
            total_missions = df_missions.groupby('hero_id').size().reset_index(name='total_missions')
            last_mission_date = df_missions.groupby('hero_id')['end_date'].max().reset_index(name='last_mission_date')
            
            # Merge das agregações com a tabela de heróis
            df_dim_heroes = df_heroes.merge(total_missions, on='hero_id', how='left')
            df_dim_heroes = df_dim_heroes.merge(last_mission_date, on='hero_id', how='left')
            
            # Salvar a tabela dimensão
            df_dim_heroes.to_parquet(f"{self.data_folder}/analytics/analytics_dim_heroes.parquet", index=False)
            logging.info("Tabela dim_heroes salva em parquet com sucesso.") 
        except Exception as e:
            logging.error(f"Erro ao tentar realizar transformações para tabela dim de heróis: {e}")
            raise
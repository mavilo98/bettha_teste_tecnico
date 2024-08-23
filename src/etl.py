import logging
import os
from datetime import *
from utils import PipelineHerois

# Definição da timestamp para registro do log
date_stamp = datetime.now().strftime("%Y%m%d%H%M%S")
data_hoje = datetime.now().strftime("%Y%m%d")

# Configuração do logging
log_dir = f"logs/{data_hoje}"
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(log_dir, f'{date_stamp}_etl.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)



def main() -> None:

    # Inicialização da pipeline de processamento de dados
    herois = PipelineHerois()

    # Leitura e integração dos arquivos da landing
    landing_integrada = herois.integrar_dados('landing', 'dc', 'marvel')
    
    # Deduplicar os registros de heróis e missões
    desdup_campos = {'heroes': 'hero_id',
                     'missions': 'mission_id'}
    landing_deduplicados = herois.deduplicar_dados(landing_integrada, desdup_campos)

    # Salva as tabelas na camada raw
    herois.salvar_dados(landing_deduplicados, 'raw')

    # Adequa o schema e salva as tabelas na camada trusted
    herois.adequar_schema_e_salva_na_trusted('raw', 'trusted')

    # Salva as dimensões sem transformações na camada analytics
    herois.salva_na_analytics('trusted', 'analytics', 'skills', 'super_powers', 'villains')

    # Realiza as transformações necessárias e salva a fact_missions e dim_heroes na camada analytics
    herois.transformar_e_salvar_fact_missions()
    herois.transformar_e_salvar_dim_heroes()

if __name__ == '__main__':
    main()
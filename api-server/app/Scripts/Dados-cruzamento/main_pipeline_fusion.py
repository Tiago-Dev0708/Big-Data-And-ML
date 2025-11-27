import pandas as pd
import numpy as np
from scipy.spatial import cKDTree
import os

CAMINHO_POCOS_ANP = r'caminho/para/arquivo_anp.csv'  

CONFIG_ESTADOS = {
    'RJ': {
        'mag': r'caminho/para/arquivo_mag.csv', 
        'gama': r'caminho/para/arquivo_gama.csv',                 
        'obs': 'Fusão Dupla (Terra)'
    },
    'MA': {
        'mag': r'caminho/para/arquivo_mag.csv', 
        'gama': None,                    
        'obs': 'Híbrido (Mag + Gama juntos)'
    },
    'PR': {
        'mag': r'caminho/para/arquivo_mag.csv', 
        'gama': r'caminho/para/arquivo_gama.csv', 
        'obs': 'Fusão Dupla (Terra)'
    },
    'BA': {
        'mag': r'caminho/para/arquivo_mag.csv', 
        'gama': r'caminho/para/arquivo_gama.csv', 
        'obs': 'Fusão Dupla (Terra)'
    }
}


def processar_estado(sigla, config, df_pocos_bruto):
    print(f"\nPROCESSANDO ESTADO: {sigla} ({config['obs']})")

    df_pocos = df_pocos_bruto[df_pocos_bruto['ESTADO'] == sigla].copy()
    if df_pocos.empty:
        print(f"Aviso: Nenhum poço encontrado para {sigla} na tabela ANP.")
        return None
    
    df_pocos.reset_index(drop=True, inplace=True)
    print(f"   -> Poços alvo: {len(df_pocos)}")

    try:
        if not os.path.exists(config['mag']):
            print(f"ERRO: Arquivo não encontrado no disco: {config['mag']}")
            return None

        df_mag = pd.read_csv(config['mag'])
        rename_map = {}
        for c in df_mag.columns:
            if c.upper() in ['LAT', 'LATITUDE', 'Y']: rename_map[c] = 'LATITUDE'
            if c.upper() in ['LONG', 'LONGITUDE', 'X']: rename_map[c] = 'LONGITUDE'
        df_mag.rename(columns=rename_map, inplace=True)

        print("Cruzando MAG")
        tree_mag = cKDTree(df_mag[['LONGITUDE', 'LATITUDE']].values)
        dist, idx = tree_mag.query(df_pocos[['LONG_POCO', 'LAT_POCO']].values, k=1)
        cols_mag_uteis = [c for c in df_mag.columns if c not in ['LATITUDE', 'LONGITUDE']]
        df_vizinhos_mag = df_mag.iloc[idx][cols_mag_uteis].reset_index(drop=True)
        df_parcial = pd.concat([df_pocos, df_vizinhos_mag], axis=1)
        df_parcial['DIST_MAG_GRAUS'] = dist
        
    except Exception as e:
        print(f"ERRO ao processar MAG: {e}")
        return None
    if config['gama']:
        try:
            if not os.path.exists(config['gama']):
                print(f"ERRO: Arquivo Gama configurado mas não encontrado: {config['gama']}")

            print("Cruzando GAMA")
            df_gama = pd.read_csv(config['gama'])
            rename_map_g = {}
            for c in df_gama.columns:
                if c.upper() in ['LAT', 'LATITUDE', 'Y']: rename_map_g[c] = 'LATITUDE'
                if c.upper() in ['LONG', 'LONGITUDE', 'X']: rename_map_g[c] = 'LONGITUDE'
            df_gama.rename(columns=rename_map_g, inplace=True)

            tree_gama = cKDTree(df_gama[['LONGITUDE', 'LATITUDE']].values)
            dist_g, idx_g = tree_gama.query(df_pocos[['LONG_POCO', 'LAT_POCO']].values, k=1)
            cols_gama_uteis = [c for c in df_gama.columns if c not in ['LATITUDE', 'LONGITUDE']]
            df_vizinhos_gama = df_gama.iloc[idx_g][cols_gama_uteis].reset_index(drop=True)
            df_final = pd.concat([df_parcial, df_vizinhos_gama], axis=1)
            
        except Exception as e:
            print(f"ERRO ao processar GAMA: {e}")
            return df_parcial
    else:
        df_final = df_parcial

    return df_final

def main():
    print("Cruzamento de Dados Geofísicos com Tabela de Poços da ANP Iniciado")
    
    print(f"Carregando Tabela ANP: {CAMINHO_POCOS_ANP}")
    try:
        df_pocos_all = pd.read_csv(CAMINHO_POCOS_ANP, sep=',', encoding='latin1', low_memory=False)
    except FileNotFoundError:
        print("ERRO: Arquivo de poços da ANP não encontrado. Verifique o caminho.")
        return
    mapeamento = {
        'POCO': 'ID_POCO', 
        'LATITUDE_BASE_DD': 'LAT_POCO', 
        'LONGITUDE_BASE_DD': 'LONG_POCO',
        'PROFUNDIDADE_VERTICAL_M': 'PROFUNDIDADE', 
        'RECLASSIFICACAO': 'TARGET_RAW', 
        'BACIA': 'BACIA',
        'ESTADO': 'ESTADO',
        'TERRA_MAR': 'AMBIENTE',          
        'TÉRMINO': 'DATA_CONCLUSAO',      
        'LAMINA_D_AGUA_M': 'LAMINA_AGUA'   
    }

    colunas_validas = [c for c in mapeamento.keys() if c in df_pocos_all.columns]
    df_pocos_all = df_pocos_all[colunas_validas].rename(columns=mapeamento)
    print(" Corrigindo separador decimal (vírgula -> ponto)...")
    cols_coords = ['LAT_POCO', 'LONG_POCO', 'PROFUNDIDADE', 'LAMINA_AGUA']
    
    for col in cols_coords:
        if col in df_pocos_all.columns:
            df_pocos_all[col] = df_pocos_all[col].astype(str).str.replace(',', '.')
            df_pocos_all[col] = pd.to_numeric(df_pocos_all[col], errors='coerce')

    def get_target(x): 
        return 1 if any(s in str(x).upper() for s in ['PETROLEO', 'GAS', 'PRODUTOR', 'COMERCIAL']) else 0
    
    df_pocos_all['TARGET'] = df_pocos_all['TARGET_RAW'].apply(get_target)
    df_pocos_all.dropna(subset=['LAT_POCO', 'LONG_POCO'], inplace=True)

    lista_dfs = []
    
    for sigla, config in CONFIG_ESTADOS.items():
        df_estado = processar_estado(sigla, config, df_pocos_all)
        if df_estado is not None:
            lista_dfs.append(df_estado)

    if lista_dfs:
        print("Consolidando Dataset Nacional")
        df_brasil = pd.concat(lista_dfs, ignore_index=True)
        
        arquivo_final = 'dataset_BRASIL_completo_ML.csv'
        df_brasil.to_csv(arquivo_final, index=False)
        
        print(f"Dataset gerado: {arquivo_final}")
        print(f"Dimensões: {df_brasil.shape[0]} linhas x {df_brasil.shape[1]} colunas")
    else:
        print("Nenhum dado foi processado com sucesso. Verifique os caminhos.")

if __name__ == "__main__":
    main()
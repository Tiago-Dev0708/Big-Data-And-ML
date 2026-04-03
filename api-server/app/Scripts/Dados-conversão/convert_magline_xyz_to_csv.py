import pandas as pd
import numpy as np

def converter_magline_xyz_to_csv(caminho_entrada, caminho_saida):
    print(f"Carregando MAGLINE na memória: {caminho_entrada}")
    

    colunas_originais = [
        'X', 'Y', 'GPSALT', 'BARO', 'ALTURA', 'MDT', 
        'LONGITUDE', 'LATITUDE', 'DATA', 'HORA', 'FID', 
        'IGRF', 'MAGBASE', 'MAGBRU', 'MAGCOM', 'MAGCOR', 
        'MAGIGRF', 'MAGMIC', 'MAGNIV'
    ]

    try:
        df = pd.read_csv(
            caminho_entrada,
            sep=r'\s+',
            names=colunas_originais,
            comment='/',
            engine='c',
            low_memory=False
        )
        
        print(f"Leitura total: {len(df)} linhas.")
        df['LONGITUDE'] = pd.to_numeric(df['LONGITUDE'], errors='coerce')
        df = df.dropna(subset=['LONGITUDE'])
        df_final = df[['LONGITUDE', 'LATITUDE', 'MAGMIC', 'ALTURA', 'MDT']].copy()
        df_final.rename(columns={
            'MAGMIC': 'MAG_VALOR', 
            'ALTURA': 'ALTURA_VOO',
            'MDT': 'TOPOGRAFIA'
        }, inplace=True)

        print(f"Linhas válidas após limpeza: {len(df_final)}")
        print("Colunas mantidas: LONGITUDE, LATITUDE, MAG_VALOR, ALTURA_VOO, TOPOGRAFIA")
        print("Salvando CSV")
        df_final.to_csv(caminho_saida, index=False)
        print(f"Magline Salvo: {caminho_saida}")

    except Exception as e:
        print(f"Erro: {e}")

converter_magline_xyz_to_csv("caminho/para/arquivo.xyz", 'geofisica_ESTADO_MAG_limpo.csv')
import pandas as pd
import numpy as np

def converter_gamaline_xyz_to_csv(caminho_entrada, caminho_saida):
    print(f"Carregando GAMALINE na memória: {caminho_entrada}")
    
    colunas_gama = [
        'X', 'Y', 'FIDUCIAL', 'GPSALT', 'BARO', 'ALTURA', 'MDT', 
        'CTB', 'KB', 'UB', 'THB', 'UUP', 'LIVE_TIME', 'COSMICO', 'TEMP', 
        'CTCOR', 'KCOR', 'UCOR', 'THCOR', 'CTEXP', 
        'Kperc', 'eU', 'eTh', 
        'THKRAZAO', 'UKRAZAO', 'UTHRAZAO', 
        'LONGITUDE', 'LATITUDE', 'DATA', 'HORA'
    ]

    try:
        df = pd.read_csv(
            caminho_entrada,
            sep=r'\s+',          
            names=colunas_gama,  
            comment='/',         
            engine='c',
            low_memory=False
        )
        
        print(f"Leitura total: {len(df)} linhas.")
        exemplo_long = df['LONGITUDE'].iloc[0]
        print(f"Validação (Amostra Longitude): {exemplo_long}")
        df['X'] = pd.to_numeric(df['X'], errors='coerce')
        df = df.dropna(subset=['X'])
        df_final = df[['LONGITUDE', 'LATITUDE', 'eTh', 'eU', 'Kperc']].copy()
        
        df_final.rename(columns={
            'eTh': 'THC',  # Tório
            'eU':  'UC',   # Urânio
            'Kperc': 'KC'  # Potássio
        }, inplace=True)

        print(f"Linhas válidas: {len(df_final)}")
        
        print("Salvando CSV")
        df_final.to_csv(caminho_saida, index=False)
        print(f"GamaLine salvo: {caminho_saida}")

    except Exception as e:
        print(f"Erro: {e}")

converter_gamaline_xyz_to_csv("caminho/para/arquivo.xyz", 'geofisica_ESTADO_GAMA_limpo.csv')
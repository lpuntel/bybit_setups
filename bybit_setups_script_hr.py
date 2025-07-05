# === IMPORTAÇÕES NECESSÁRIAS ===
import pandas as pd
from datetime import datetime
import requests
from pybit.unified_trading import HTTP
import logging
import mplfinance as mpf
from openpyxl import Workbook
from openpyxl.drawing.image import Image as OpenpyxlImage
from openpyxl.utils.dataframe import dataframe_to_rows
from PIL import Image
import io
import matplotlib
import matplotlib.pyplot as plt
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from io import BytesIO
from dotenv import load_dotenv
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from oauth2client.service_account import ServiceAccountCredentials  # not used for OAuth, just illustration
from oauth2client.client import OAuth2Credentials
import os

# Desativa logs de DEBUG do matplotlib e mplfinance
logging.getLogger('matplotlib').setLevel(logging.WARNING)
logging.getLogger('PIL').setLevel(logging.WARNING)
matplotlib.set_loglevel('warning')  # Apenas se sua versão do matplotlib suportar

# === CONFIGURAÇÕES INICIAIS ===
PERIODOS_TENDENCIA = 10  # Número de candles para confirmar tendência predominante (usado no 9.1)
PERIODOS_SEQUENCIA_TENDENCIA = 10  # Número de candles consecutivos para confirmar sequência de tendência (9.2, 9.3, 9.4, PC)
PERIODOS_TENDENCIA_SUAVE = 6

PASSO_TENDENCIA_SUAVE = 2  # Intervalo usado para suavizar a comparação entre médias (ex: compara -9 com -11)
PERIODOS_MINIMO = 30 #Número mínimo de períodos para considerar análise do ativo 
CASAS_DECIMAIS_GATILHO = 7  # Número de casas decimais para exibir os gatilhos
ENVIAR_ALERTA_TELEGRAM = True  # Enviar alertas automáticos via Telegram
DEBUG_MODE = True  # Para exibir mensagens detalhadas no futuro (opcional)

# Parâmetros de Horário de Execução
USAR_HORARIO_LOCAL = True
HORA_INICIO = 0
HORA_FIM = 23
INTERVALO_MINUTOS = 1
PERMITIR_FIM_DE_SEMANA = True

# === CAMINHO DO ARQUIVO EXCEL COM ATIVOS ===
# >>> AJUSTE O CAMINHO CONFORME SEU COMPUTADOR <<<
ARQUIVO_EXCEL = "ativos.xlsx"

#Parametros para salvar ativos_opt.xlsx na pasta do Google Drive 
#GDRIVE_CLIENT_ID = "328835830437-dknbuvpdh7ttfg5g0r16v5u2diibe4ns.apps.googleusercontent.com"
#GDRIVE_CLIENT_SECRET = "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"
#GDRIVE_REFRESH_TOKEN = "1//0hVA1K5twRXF8CgYIARAAGBESNwF-L9IrDKJPi9ych-oppnHYWAeKXqjZSqCduG_6o0H6nthTn0-84Pux5tcWIsIQAa_P_4e-4Kc"
#GDRIVE_FOLDER_ID = "1OihZOcMsg6JuDuo7OuOw6LXgWlEeEI7W?usp=drive_link"

"""
Este script identifica os setups 9.1 a 9.4 (Larry Williams) e PC (Ponto Contínuo),
aplicando a lógica:
- Candle [-1]: ARMAR o gatilho
- Candle [0]: DISPARAR, se houver rompimento
- Setups 9.2, 9.3 e PC admitem escorregamento de gatilho
"""

# Configuração central de logging
logging.basicConfig(
    level=logging.DEBUG if DEBUG_MODE else logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)

# Acrescenta logs também em arquivo (append)
log_file_path = os.path.join(os.path.dirname(__file__), 'log_cron.txt')
file_handler = logging.FileHandler(log_file_path, mode='a')
file_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S'))
logging.getLogger().addHandler(file_handler)

# Lista que armazenará dados de integridade dos candles
dados_integridade = []

# === FUNÇÕES AUXILIARES ===
# Função para checar tendência predominante (?)
def tendencia_predominante(mme, tipo='alta'):
    if tipo == 'alta':
        return mme.iloc[-PERIODOS_TENDENCIA - 2] < mme.iloc[-2]
    elif tipo == 'baixa':
        return mme.iloc[-PERIODOS_TENDENCIA - 2] > mme.iloc[-2]
    return False

# Função para checar sequência de tendência (para Setups 9.1)
def tendencia_sequencia(mme, tipo='alta'):
    if tipo == 'alta':
        return all(mme.iloc[i] < mme.iloc[i+1] for i in range(-PERIODOS_SEQUENCIA_TENDENCIA-1, -2))
    elif tipo == 'baixa':
        return all(mme.iloc[i] > mme.iloc[i+1] for i in range(-PERIODOS_SEQUENCIA_TENDENCIA-1, -2))
    return False

# === Função de tendência suavizada (para Setups 9.2, 9.3, 9.4, PC) ===
def tendencia_suave(mme, tipo='alta', passo=2, periodo=PERIODOS_TENDENCIA_SUAVE):
    """
    Verifica tendência suavizada com comparação espaçada (ex: [-9] > [-11], etc.)
    Ideal para setups sensíveis a microcorreções (como 9.2, 9.3, 9.4 e PC).
    """
    if len(mme) < periodo + passo:
        return False

    for i in range(-periodo - passo + 1, -passo + 2):
        if tipo == 'alta' and not (mme.iloc[i] > mme.iloc[i - passo]):
            return False
        if tipo == 'baixa' and not (mme.iloc[i] < mme.iloc[i - passo]):
            return False

    return True

# Funções auxiliares de exportação e gráficos
def gerar_excel_com_graficos(candles_dict, ativos_df, nome_arquivo='ativos_opt.xlsx'):
    writer = pd.ExcelWriter(nome_arquivo, engine='xlsxwriter')
    workbook = writer.book

    # === ABA 1: Tabela de resultados ===
    colunas_saida = [
        'Par', 'Timeframe', 'Mercado', 'Time Stamp', 'Setup', 'COMPRA/VENDA', 'ARMAR/DISPARAR',
        'GATILHO', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'MME9', 'MMA21', 'VOLUME', 'VOLUME_MMA21', 'CLOSE_ZERO'
    ]
    tabela_saida = []

    for _, linha in ativos_df.iterrows():
        resultado = linha['Último Setup Identificado']
        if resultado.startswith('ARMAR') or resultado.startswith('DISPARAR'):
            par = linha['Par']
            df = candles_dict.get(par)
            if df is None or len(df) < 21:
                continue

            # Cálculo da média móvel de 21 períodos do volume
            df['VOLUME_MMA21'] = df['volume'].rolling(window=21).mean()
            candle_zero = df.iloc[-1]
            candle_m1 = df.iloc[-2]

            try:
                partes = resultado.split(' ')
                status = partes[0]
                setup = partes[2]
                direcao = partes[1]  # COMPRA ou VENDA
                gatilho = float(resultado.split('gatilho: ')[1].split(' ')[0])
            except:
                continue
            
            tabela_saida.append([
                par,
                linha['Timeframe'],
                linha['Mercado'],
                candle_zero['timestamp'].strftime('%d/%m/%Y %H:%M'),
                setup,
                direcao,
                status.capitalize(),
                gatilho,
                candle_m1['open'],
                candle_m1['high'],
                candle_m1['low'],
                candle_m1['close'],
                candle_m1.get('MME9', None),
                candle_m1.get('MMA21', None),
                float(candle_m1.get('volume', 0) or 0),
                candle_m1.get('VOLUME_MMA21', None),
                candle_zero['close']
            ])

    df_saida = pd.DataFrame(tabela_saida, columns=colunas_saida)
    df_saida.to_excel(writer, sheet_name='Setups Identificados', index=False, startrow=1, header=False)

    # Formatação de cabeçalhos
    worksheet_tabela = writer.sheets['Setups Identificados']
    for col_num, value in enumerate(df_saida.columns.values):
        worksheet_tabela.write(0, col_num, value)

    # Formatação com vírgula decimal
    formato_decimal = workbook.add_format({'num_format': '#,##0.00000000'})
    formato_volume = workbook.add_format({'num_format': '#,##0'})

    colunas_float = ['GATILHO', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'MME9', 'MMA21', 'VOLUME','VOLUME_MMA21', 'CLOSE_ZERO']
    for col_nome in colunas_float:
        col_idx = df_saida.columns.get_loc(col_nome)
        worksheet_tabela.set_column(col_idx, col_idx, 18, formato_decimal)

    # === ABA 2: Gráficos ===
    worksheet = workbook.add_worksheet('Graficos')
    writer.sheets['Graficos'] = worksheet

    linha_atual = 0

    for _, linha in ativos_df.iterrows():
        resultado = linha['Último Setup Identificado']
        par = linha['Par']

        if not (resultado.startswith('ARMAR') or resultado.startswith('DISPARAR')):
            continue

        df = candles_dict.get(par)
        if df is None or len(df) < 13:
            continue

        df_plot = df.iloc[-13:].copy()
        df_plot.set_index('timestamp', inplace=True)
        df_plot.index.name = 'Date'

        for col in ['open', 'high', 'low', 'close', 'volume']:
            if col in df_plot.columns:
                df_plot[col] = pd.to_numeric(df_plot[col], errors='coerce')

        if df_plot[['open', 'high', 'low', 'close']].isnull().any().any():
            continue

        apds = []
        if 'MME9' in df_plot.columns:
            apds.append(mpf.make_addplot(df_plot['MME9'], color='blue'))
        if 'MMA21' in df_plot.columns:
            apds.append(mpf.make_addplot(df_plot['MMA21'], color='orange'))

        try:
            img_data = BytesIO()
            mpf.plot(
                df_plot,
                type='candle',
                style='yahoo',
                addplot=apds,
                title=par,
                ylabel='Preço',
                figsize=(8, 4),
                savefig=dict(fname=img_data, dpi=100, format='png')
            )
            img_data.seek(0)
            worksheet.insert_image(linha_atual, 0, '', {'image_data': img_data})
            linha_atual += 21
        except Exception as e:
            print(f"[❌] Erro ao gerar gráfico para {par}: {e}")
            continue

    writer._save()
#    print(f"✅ Arquivo gerado: {nome_arquivo}")

# Função para buscar candles da Bybit
def obter_candles(par='BTCUSDT', interval='15', limit=50, mercado='linear'):
    session = HTTP(testnet=False)
    dados = session.get_kline(
        category=mercado,
        symbol=par,
        interval=interval,
        limit=limit
    )
    df = pd.DataFrame(dados['result']['list'], columns=[
        'timestamp', 'open', 'high', 'low', 'close', 'volume', 'turnover'
    ])
    df['open'] = df['open'].astype(float)
    df['close'] = df['close'].astype(float)
    df['high'] = df['high'].astype(float)
    df['low'] = df['low'].astype(float)
    df['timestamp'] = pd.to_datetime(pd.to_numeric(df['timestamp']), unit='ms')
    df = df.iloc[::-1]      # ordena em ordem cronológica (do mais antigo ao mais recente)
#    df = df[:-1]           #cancela a remoção do [0] anulando>>> # remove o último candle (em formação)>>[-1] representa o último candle fechado de verdade

    return df

# Função para enviar alerta via Telegram
def enviar_alerta_telegram(mensagem):
    bot_token = '7564918167:AAGm5HpWXGQ3boiKgaivan3_JSsmv-fO4a0'
    chat_id = '6883823169'
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {'chat_id': chat_id, 'text': mensagem}
    try:
        if ENVIAR_ALERTA_TELEGRAM:
            requests.post(url, data=payload)
    except Exception as e:
        print(f"Erro ao enviar alerta Telegram: {e}")

# Função para configurar horário de execução
from datetime import datetime

def dentro_do_horario():
    agora = datetime.now() if USAR_HORARIO_LOCAL else datetime.utcnow()
    hora = agora.hour
    minuto = agora.minute
    dia_semana = agora.weekday()

    # Calcula o "erro" entre o minuto atual e o mais próximo múltiplo de intervalo
    erro_minuto = abs(minuto % INTERVALO_MINUTOS)
    margem_erro = 1  # Aceita 1 minuto de tolerância

    logging.debug(f"    Condições: "
          f"permite dia útil (dia) ou fim de semana (True)? {dia_semana < 5 or PERMITIR_FIM_DE_SEMANA}, "
          f"hora entre {HORA_INICIO}-{HORA_FIM}? {HORA_INICIO <= hora <= HORA_FIM}, "
          f"minuto dentro de tolerância ±{margem_erro}? {erro_minuto <= margem_erro or INTERVALO_MINUTOS - erro_minuto <= margem_erro}")

    if not PERMITIR_FIM_DE_SEMANA and dia_semana >= 5:
        return False

    if HORA_INICIO <= hora <= HORA_FIM:
        if erro_minuto <= margem_erro or INTERVALO_MINUTOS - erro_minuto <= margem_erro:
            return True

    return False

#Salvar no Google Drive a partir do Hostinger
load_dotenv()
def enviar_para_google_drive(nome_arquivo_local):
    print(f"[INFO] Iniciando envio de '{nome_arquivo_local}' para o Google Drive...")

    client_id = os.getenv("GDRIVE_CLIENT_ID")
    client_secret = os.getenv("GDRIVE_CLIENT_SECRET")
    refresh_token = os.getenv("GDRIVE_REFRESH_TOKEN")
    folder_id = os.getenv("GDRIVE_FOLDER_ID")

    if not all([client_id, client_secret, refresh_token]):
        print("[❌] Variáveis de ambiente do Google Drive não configuradas corretamente.")
        return

    # Define o dicionário completo com todos os campos exigidos pelo PyDrive2
    gauth = GoogleAuth()
    gauth.settings['client_config_backend'] = 'settings'
    gauth.settings['client_config'] = {
        "client_id": client_id,
        "client_secret": client_secret,
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "revoke_uri": "https://oauth2.googleapis.com/revoke"
    }

    gauth.settings['save_credentials'] = False
    gauth.settings['oauth_scope'] = ["https://www.googleapis.com/auth/drive.file"]

    # Força autenticação com refresh token
    from oauth2client.client import OAuth2Credentials
    gauth.credentials = OAuth2Credentials(
        access_token=None,
        client_id=client_id,
        client_secret=client_secret,
        refresh_token=refresh_token,
        token_expiry=None,
        token_uri="https://oauth2.googleapis.com/token",
        user_agent=None,
        revoke_uri="https://oauth2.googleapis.com/revoke"
    )

    drive = GoogleDrive(gauth)

    # Envia arquivo
    file_drive = drive.CreateFile({
        'title': nome_arquivo_local,
        'parents': [{'id': folder_id}] if folder_id else []
    })
    file_drive.SetContentFile(nome_arquivo_local)
    file_drive.Upload()

    print(f"[✅] Arquivo '{nome_arquivo_local}' enviado com sucesso ao Google Drive.")

def upload_file_to_drive(local_file_path, drive_folder_id):
    """
    Faz upload de um arquivo para o Google Drive, substituindo-o se já existir.
    - local_file_path: caminho do arquivo local a enviar (ex: './ativos_opt_hr.xlsx')
    - drive_folder_id: ID da pasta de destino no Google Drive (string).
    """
    # 1. Ler credenciais sensíveis das variáveis de ambiente
    client_id = os.environ.get("GDRIVE_CLIENT_ID")
    client_secret = os.environ.get("GDRIVE_CLIENT_SECRET")
    refresh_token = os.environ.get("GDRIVE_REFRESH_TOKEN")
    if not client_id or not client_secret or not refresh_token:
        raise RuntimeError("Credenciais do Google Drive não encontradas nas variáveis de ambiente.")

    # 2. Configurar autenticação GoogleAuth com OAuth2 (modo headless usando refresh token)
    gauth = GoogleAuth()
    # Definir as configurações do cliente OAuth diretamente (sem arquivo client_secrets.json)
    gauth.settings["client_config_backend"] = "settings"
    gauth.settings["client_config"] = {
        "client_id": client_id,
        "client_secret": client_secret,
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://accounts.google.com/o/oauth2/token",
        "redirect_uri": "urn:ietf:wg:oauth:2.0:oob"  # URI de redirecionamento padrão para apps instalados
    }
    # Criar credencial OAuth2 usando o refresh token (access_token será obtido automaticamente)
    credentials = OAuth2Credentials(
        None,            # access_token inicial (None para forçar refresh imediato)
        client_id,
        client_secret,
        refresh_token,
        None,            # data de expiração (None porque vamos sempre atualizar)
        "https://accounts.google.com/o/oauth2/token",
        None            # user_agent (opcional)
    )
    gauth.credentials = credentials  # Atribuir as credenciais ao objeto GoogleAuth

    # Garantir que estamos autenticados (refresh o token de acesso se necessário)
    if gauth.access_token_expired:
        gauth.Refresh()      # Usa o refresh token para obter um novo access token
    else:
        gauth.Authorize()    # Credencial já válida (não expirada)

    # Criar a instância do GoogleDrive autenticada
    drive = GoogleDrive(gauth)

    # 3. Verificar se o arquivo já existe na pasta de destino
    file_name = os.path.basename(local_file_path)
    query = f"title='{file_name}' and '{drive_folder_id}' in parents and trashed=false"
    file_list = drive.ListFile({'q': query}).GetList()
    # O query acima busca por arquivos com título igual ao nome do arquivo e na pasta especificada (não removidos)

    if file_list:
        # Arquivo já existe – pegar o primeiro resultado (assumindo nome único)
        existing_file = file_list[0]
        print(f"Arquivo encontrado no Drive (ID={existing_file['id']}), atualizando conteúdo...")
        existing_file.SetContentFile(local_file_path)  # Define o conteúdo para o arquivo local
        existing_file.Upload()  # Faz upload sobrescrevendo o conteúdo do arquivo existente:contentReference[oaicite:6]{index=6}
        print(f"Arquivo '{file_name}' atualizado com sucesso no Google Drive.")
    else:
        # Arquivo não existe – criar um novo na pasta alvo
        print(f"Arquivo não encontrado no Drive, fazendo upload como novo arquivo.")
        new_file = drive.CreateFile({'title': file_name, 'parents': [{'id': drive_folder_id}]})
        new_file.SetContentFile(local_file_path)  # Anexa o conteúdo do arquivo local
        new_file.Upload()  # Envia o arquivo (novo upload) para o Drive:contentReference[oaicite:7]{index=7}
        print(f"Arquivo '{file_name}' enviado com sucesso para a pasta do Drive.")

# === Setups de Larry Williams ===
# === SETUP 9.1 ===
def setup_9_1(df, ativo=""):
    df = df.copy()
    candle_zero = df.iloc[-1]  # Candle [0] em formação
    df = df.iloc[:-1]          # Remove o candle em formação

    df['MME9'] = df['close'].ewm(span=9).mean()
    mme = df['MME9']

    logging.debug(f"    ANALISANDO 9.1")         

    # COMPRA
    if tendencia_sequencia(mme, 'baixa'):
        logging.debug(f"🔽 Tendência de baixa confirmada. E a reversão?")

        if mme.iloc[-2] < mme.iloc[-1]:
            c1 = df.iloc[-1]
            c2 = df.iloc[-2]
            c3 = df.iloc[-3]

            logging.debug(f"Candle [-1] (c1): close={c1['close']}, low={c1['low']}, high={c1['high']}")
            logging.debug(f"[Candle [-2] (c2): close={c2['close']}, low={c2['low']}")
            logging.debug(f"[Candle [-3] (c3): close={c3['close']}, low={c3['low']}")
            logging.debug(f"[Candle [0] (zero): low={candle_zero['low']}, high={candle_zero['high']}")

            gatilho = df['high'].iloc[-1]
            if candle_zero['high'] > c1['high']:
                logging.debug(f" ✅ Condição de COMPRA atendida - Gatilho em high[-2] = {gatilho:.7f}")
                return {
                    'status': 'DISPARAR COMPRA 9.1',
                    'gatilho': gatilho,
                    'tipo': 'compra',
                    'coluna': 'high'
                }
            return {
                'status': 'ARMAR COMPRA 9.1',
                'gatilho': gatilho,
                'tipo': 'compra',
                'coluna': 'high'
            }
        logging.debug(f"    Reversão para alta não identificada")

    else: logging.debug(f"    Tendência de baixa não identificada")

# VENDA
    if tendencia_sequencia(mme, 'alta'):
        logging.debug(f"🔼 Tendência de alta confirmada. E a reversão?")

        if mme.iloc[-2] > mme.iloc[-1]:
            c1 = df.iloc[-1]
            c2 = df.iloc[-2]
            c3 = df.iloc[-3]

            logging.debug(f"Candle [-1] (c1): close={c1['close']}, low={c1['low']}, high={c1['high']}")
            logging.debug(f"Candle [-2] (c2): close={c2['close']}, low={c2['low']}")
            logging.debug(f"Candle [-3] (c3): close={c3['close']}, low={c3['low']}")
            logging.debug(f"Candle [0] (zero): low={candle_zero['low']}, high={candle_zero['high']}")

            gatilho = df['low'].iloc[-1]
            if candle_zero['low'] < c1['low']:
                logging.debug(f"✅ Condição de VENDA atendida - Gatilho em low[-2] = {gatilho:.7f}")
                return {
                    'status': 'DISPARAR VENDA 9.1',
                    'gatilho': gatilho,
                    'tipo': 'venda',
                    'coluna': 'low'
                }
            return {
                'status': 'ARMAR VENDA 9.1',
                'gatilho': gatilho,
                'tipo': 'venda',
                'coluna': 'low'
            }
        logging.debug(f"    Reversão para baixa não identificada")

    logging.debug(f"    Tendência de alta não identificada")

    return None

# === SETUP 9.2 ===
def setup_9_2(df, ativo=""):
    df = df.copy()
    candle_zero = df.iloc[-1]  # Candle [0] ainda em formação
    df = df.iloc[:-1]          # Remove o candle [0]; agora [-1] é o último fechado
    df['MME9'] = df['close'].ewm(span=9).mean()
    mme = df['MME9']

    logging.debug(f"    ANALISANDO 9.2")         

    # === COMPRA ===
    if tendencia_suave(mme, tipo='alta', passo=PASSO_TENDENCIA_SUAVE, periodo=PERIODOS_TENDENCIA_SUAVE):
        logging.debug(f"🔽 Tendência de alta confirmada")
        
        c1 = df.iloc[-1]
        c2 = df.iloc[-2]
        c3 = df.iloc[-3]

        logging.debug(f"    Candle [-1] (c1): close={c1['close']}, high={c1['high']}")
        logging.debug(f"    Candle [-2] (c2): close={c2['close']}, high={c2['high']}, low={c2['low']}")
        logging.debug(f"    Candle [-3] (c3): low={c3['low']}")
        logging.debug(f"    Candle [0] (zero): high={candle_zero['high']}, close={candle_zero['close']}")
       
        if c1['close'] < c2['low'] and c2['open'] < c2['close']: # Candle anterior é de ALTA
            if candle_zero['high'] > c1['high']:
                return {
                    'status': 'DISPARAR COMPRA 9.2',
                    'gatilho': c1['high'],
                    'tipo': 'compra',
                    'coluna': 'high',
                    'debug_origem': 'COMPRA-DISPARAR'
                }
            return {
                'status': 'ARMAR COMPRA 9.2',
                'gatilho': c1['high'],
                'tipo': 'compra',
                'coluna': 'high',
                'debug_origem': 'COMPRA-ARMAR'
            }
    
        print("-" * 92)   
        logging.debug(f"    Iniciando avaliação de escorregamento")
        candle_m1 = df.iloc[-1]
        candle_m2 = df.iloc[-2]

        if candle_m2['high'] > candle_m1['high']:
            for i in range(-2, -7, -1):
                if abs(i) > len(df) - 1:
                    break
                atual = df.iloc[i]
                anterior = df.iloc[i - 1]

                logging.debug(f"    i={i} | atual.close={atual['close']} | anterior.low={anterior['low']}")
                logging.debug(f"    Verificando continuidade: atual.high={atual['high']} vs anterior.high={anterior['high']}")
    
                if atual['close'] < anterior['low'] and anterior['open'] < anterior['close']: # Candle anterior é de ALTA
                    logging.debug(f" ↪️ Escorregamento válido encontrado em i={i}")
                    if candle_zero['high'] > c1['high']:
                        return {
                            'status': f'DISPARAR COMPRA 9.2',
                            'gatilho': c1['high'],
                            'tipo': 'compra',
                            'coluna': 'high',
                            'debug_origem': f'COMPRA-ESCORREGA-DISPARAR-i{i}'
                        }
                    return {
                        'status': f'ARMAR COMPRA 9.2',
                        'gatilho': c1['high'],
                        'tipo': 'compra',
                        'coluna': 'high',
                        'debug_origem': f'COMPRA-ESCORREGA-ARMAR-i{i}'
                    }
                if atual['close'] < anterior['low'] and anterior['open'] > anterior ['close']: # Candle anterior NÃO é de ALTA
                    logging.debug(f"    Não considera porque o candle anterior é de BAIXA → {anterior['open']} >= {anterior['close']}")

                if atual['high'] >= anterior['high']:
                    logging.debug(f"    Interrompe: máxima deixou de ser descendente → {atual['high']} >= {anterior['high']}")
                    break
        else:
            logging.debug(f"    Interrompe: máxima do Candle[-1]→{candle_m1['high']} >= máxima do Candle[-2]→{candle_m2['high']}")
    else: logging.debug(f"    Tendência de alta não identificada")

    # === VENDA ===
    if tendencia_suave(mme, tipo='baixa', passo=PASSO_TENDENCIA_SUAVE, periodo=PERIODOS_TENDENCIA_SUAVE):
        logging.debug(f"🔽 Tendência de baixa confirmada")
        c1 = df.iloc[-1]
        c2 = df.iloc[-2]
        c3 = df.iloc[-3]

        logging.debug(f"    Candle [-1] (c1): close={c1['close']}, low={c1['low']}")
        logging.debug(f"    Candle [-2] (c2): close={c2['close']}, low={c2['low']}, high={c2['high']}")
        logging.debug(f"    Candle [-3] (c3): high={c3['high']}")
        logging.debug(f"    Candle [0] (zero): low={candle_zero['low']}, close={candle_zero['close']}")

        if c1['close'] > c2['high'] and c2['open'] > c2['close']: # Candle anterior é de BAIXA
            if candle_zero['low'] < c1['low']:
                return {
                    'status': 'DISPARAR VENDA 9.2',
                    'gatilho': c1['low'],
                    'tipo': 'venda',
                    'coluna': 'low',
                    'debug_origem': 'VENDA-DISPARAR'
                }
            return {
                'status': 'ARMAR VENDA 9.2',
                'gatilho': c1['low'],
                'tipo': 'venda',
                'coluna': 'low',
                'debug_origem': 'VENDA-ARMAR'
            }

        print("-" * 92)   
        logging.debug(f"    Iniciando avaliação de escorregamento")
        candle_m1 = df.iloc[-1]
        candle_m2 = df.iloc[-2]

        if candle_m2['low'] < candle_m1['low']:
            for i in range(-2, -7, -1):
                if abs(i) > len(df) - 1:
                    break
                atual = df.iloc[i]
                anterior = df.iloc[i - 1]

                logging.debug(f"    i={i} | atual.close={atual['close']} | anterior.high={anterior['high']}")
                logging.debug(f"    Verificando continuidade: atual.low={atual['low']} vs anterior.low={anterior['low']}")

                if atual['close'] > anterior['high'] and anterior['open'] > anterior['close']:  # Candle anterior é de BAIXA
                    logging.debug(f" ↪️ Escorregamento válido encontrado em i={i}")
                    if candle_zero['low'] < c1['low']:
                        return {
                            'status': f'DISPARAR VENDA 9.2',
                            'gatilho': c1['low'],
                            'tipo': 'venda',
                            'coluna': 'low',
                            'debug_origem': f'VENDA-ESCORREGA-DISPARAR-i{i}'
                        }
                    return {
                        'status': f'ARMAR VENDA 9.2',
                        'gatilho': c1['low'],
                        'tipo': 'venda',
                        'coluna': 'low',
                        'debug_origem': f'VENDA-ESCORREGA-ARMAR-i{i}'
                    }

                if atual['close'] > anterior['low'] and anterior['open'] < anterior ['close']:  # Candle anterior NÃO é de BAIXA
                    logging.debug(f"    Não considera porque o candle anterior é de ALTA → {anterior['open']} < {anterior['close']}")
            
                if atual['low'] <= anterior['low']:
                    logging.debug(f"    Interrompe: mínima deixou de ser ascendente → {atual['low']} <= {anterior['low']}")
                    break
        else:
            logging.debug(f"    Interrompe: mínima do Candle[-1]→{candle_m1['low']} <= mínima do Candle[-2]→{candle_m2['low']}")
    else: logging.debug(f"    Tendência de baixa não identificada")
 
    return None

def setup_9_3(df, ativo=""):
    df = df.copy()
    candle_zero = df.iloc[-1]  # Candle [0] ainda em formação
    df = df.iloc[:-1]          # Remove o candle [0]; agora [-1] é o último fechado

    df['MME9'] = df['close'].ewm(span=9).mean()
    mme = df['MME9']

    logging.debug(f"    ANALISANDO 9.3")         

    # === COMPRA ===
    if tendencia_suave(mme, tipo='alta', passo=PASSO_TENDENCIA_SUAVE, periodo=PERIODOS_TENDENCIA_SUAVE):
        c1 = df.iloc[-1]   # c[-1]
        c2 = df.iloc[-2]   # c[-2]
        c3 = df.iloc[-3]   # c[-3]

        logging.debug(f"    c1[-1]: close={c1['close']}, high={c1['high']}")
        logging.debug(f"    c2[-2]: close={c2['close']}, high={c2['high']}")
        logging.debug(f"    c3[-3]: close={c3['close']}")
        logging.debug(f"    Candle [0]: high={candle_zero['high']}")

        if c1['close'] < c3['close'] and c2['close'] < c3['close'] and c3['open'] < c3['close']: #Candle [-3] de Alta
            if candle_zero['high'] > c1['high']:
                return {
                    'status': 'DISPARAR COMPRA 9.3',
                    'gatilho': c1['high'],
                    'tipo': 'compra',
                    'coluna': 'high',
                    'debug_origem': 'COMPRA-DISPARAR'
                }
            return {
                'status': 'ARMAR COMPRA 9.3',
                'gatilho': c1['high'],
                'tipo': 'compra',
                'coluna': 'high',
                'debug_origem': 'COMPRA-ARMAR'
            }
        
        print("-" * 92)   
        logging.debug(f"    Iniciando avaliação de escorregamento")
        candle_m1 = df.iloc[-1]
        candle_m2 = df.iloc[-2]
        candle_m3 = df.iloc[-3]

        if candle_m3['high'] > candle_m2['high'] > candle_m1['high']:
            for i in range(-3, -8, -1):  # de -3 até -7
                if abs(i + 2) > len(df):
                    break

                atual = df.iloc[i]
                prox = df.iloc[i + 1]
                prox2 = df.iloc[i + 2]

                logging.debug(f"    i={i} | atual.close={atual['close']} | prox.close={prox['close']} | prox2.close={prox2['close']}")

                if atual['close'] > prox['close'] and atual['close'] > prox2['close'] and atual['open'] < atual['close']: # Atual é Candle de ALTA
                    if candle_zero['high'] > c1['high']:
                        return {
                            'status': 'DISPARAR COMPRA 9.3',
                            'gatilho': c1['high'],
                            'tipo': 'compra',
                            'coluna': 'high',
                            'debug_origem': f'COMPRA-ESCORREGA-DISPARAR-i{i}'
                        }
                    return {
                        'status': 'ARMAR COMPRA 9.3',
                        'gatilho': c1['high'],
                        'tipo': 'compra',
                        'coluna': 'high',
                        'debug_origem': f'COMPRA-ESCORREGA-ARMAR-i{i}'
                    }

                if atual['close'] > prox['close'] and atual['close'] > prox2['close'] and atual['open'] > atual['close']: # Atual NÃO é Candle de ALTA
                    logging.debug(f"    Não considera porque o candle atual é de BAIXA → {atual['open']} >= {atual['close']}")

                if not (atual['high'] > prox['high'] > prox2['high']):
                    logging.debug(f"Interrompe: máxima deixou de ser descendente: {atual['high']} → {prox['high']} → {prox2['high']}")
                    break
        else:
            logging.debug(f"    Interrompe: máxima do Candle[-1]→{candle_m1['high']} >= máxima do Candle[-2]→{candle_m2['high']} ou máxima do Candle[-2] >= Candle[-3]→{candle_m3['high']}")

    # === VENDA ===
    if tendencia_suave(mme, tipo='baixa', passo=PASSO_TENDENCIA_SUAVE, periodo=PERIODOS_TENDENCIA_SUAVE):
        c1 = df.iloc[-1]
        c2 = df.iloc[-2]
        c3 = df.iloc[-3]
        
        logging.debug(f"    c1[-1]: close={c1['close']}, low={c1['low']}")
        logging.debug(f"    c2[-2]: close={c2['close']}, low={c2['low']}")
        logging.debug(f"    c3[-3]: close={c3['close']}")
        logging.debug(f"    Candle [0]: low={candle_zero['low']}")

        if c1['close'] > c3['close'] and c2['close'] > c3['close'] and c3['open'] > c3['close']:
            if candle_zero['low'] < c1['low']:
                return {
                    'status': 'DISPARAR VENDA 9.3',
                    'gatilho': c1['low'],
                    'tipo': 'venda',
                    'coluna': 'low',
                    'debug_origem': 'VENDA-DISPARAR'
                }
            return {
                'status': 'ARMAR VENDA 9.3',
                'gatilho': c1['low'],
                'tipo': 'venda',
                'coluna': 'low',
                'debug_origem': 'VENDA-ARMAR'
            }

        print("-" * 92)   
        logging.debug(f"    Iniciando avaliação de escorregamento")
        candle_m1 = df.iloc[-1]
        candle_m2 = df.iloc[-2]
        candle_m3 = df.iloc[-3]

        if candle_m3['low'] < candle_m2['low'] < candle_m1['low']:
            for i in range(-3, -8, -1):
                if abs(i + 2) > len(df):
                    break

                atual = df.iloc[i]
                prox = df.iloc[i + 1]
                prox2 = df.iloc[i + 2]

                logging.debug(f"    i={i} | atual.close={atual['close']} | prox.close={prox['close']} | prox2.close={prox2['close']}")

                if atual['close'] < prox['close'] and atual['close'] < prox2['close'] and atual['open'] > atual['close']:
                    if candle_zero['low'] < c1['low']:
                        return {
                            'status': 'DISPARAR VENDA 9.3',
                            'gatilho': c1['low'],
                            'tipo': 'venda',
                            'coluna': 'low',
                            'debug_origem': f'VENDA-ESCORREGA-DISPARAR-i{i}'
                        }
                    return {
                        'status': 'ARMAR VENDA 9.3',
                        'gatilho': c1['low'],
                        'tipo': 'venda',
                        'coluna': 'low',
                        'debug_origem': f'VENDA-ESCORREGA-ARMAR-i{i}'
                    }

                if atual['close'] < prox['close'] and atual['close'] < prox2['close'] and atual['open'] < atual['close']: # Atual NÃO é Candle de BAIXA
                    logging.debug(f"    Não considera porque o candle atual é de ALTA → {atual['open']} >= {atual['close']}")

                if not (atual['low'] < prox['low'] < prox2['low']):
                    logging.debug(f"    Interrompe: mínima deixou de ser ascendente: {atual['low']} → {prox['low']} → {prox2['low']}")
                    break
        else:
            logging.debug(f"    Interrompe: mínima do Candle[-1]→{candle_m1['low']} <= mínima do Candle[-2]→{candle_m2['low']} ou a mínima do Candle[-2] <= mínima do Candle[-3]→{candle_m3['low']}")

    return None

# === SETUP 9.4 ===
def setup_9_4(df, ativo=""):
    df = df.copy()
    candle_zero = df.iloc[-1]  # Candle [0] em formação
    df = df.iloc[:-1]          # Remove o candle em formação

    df['MME9'] = df['close'].ewm(span=9).mean()
    mme = df['MME9']

    logging.debug(f"    ANALISANDO 9.4")         

    # === COMPRA
    if (tendencia_suave(mme, tipo='alta', passo=PASSO_TENDENCIA_SUAVE, periodo=PERIODOS_TENDENCIA_SUAVE) and
        mme.iloc[-3] > mme.iloc[-2] and
        mme.iloc[-2] < mme.iloc[-1]):

        logging.debug(f"🔼 Tendência de alta e S confirmados")
        c1 = df.iloc[-1]
        c2 = df.iloc[-2]
        c3 = df.iloc[-3]

        if (c2['low'] < c1['low']):
            logging.debug(f"    Candle [-1] (c1): close={c1['close']}, low={c1['low']}, high={c1['high']}")
            logging.debug(f"    Candle [-2] (c2): close={c2['close']}, low={c2['low']}")
            logging.debug(f"    Candle [-3] (c3): close={c3['close']}, low={c3['low']}")
            logging.debug(f"    Candle [0] (zero): low={candle_zero['low']}, high={candle_zero['high']}")

            logging.debug(f"    ----------------------------------------------")
            logging.debug(f"    Curva S estabelecida")
            logging.debug(f"    MME9[-3]: {mme.iloc[-3]} > MME9[-2]: {mme.iloc[-2]} < MME9[-1]: {mme.iloc[-1]}")
            logging.debug(f"    Mínima do candle [-2] NÃO rompida pelo candle [-1]")
            logging.debug(f"    Candle [-1] (c1): low={c1['low']} > candle [-2]: (c2) low={c2['low']}")
            logging.debug(f"    ----------------------------------------------")

            gatilho = c1['high']
            if candle_zero['high'] >= c1['high']:
                logging.debug(f"✅ DISPARAR COMPRA 9.4")
                logging.debug(f"    Gatilho = high[-1] = {gatilho:.7f}")

                return {
                'status': 'DISPARAR COMPRA 9.4',
                'gatilho': gatilho,
                'tipo': 'COMPRA',
                'coluna': 'HIGH'
                }
                
            logging.debug(f"✅ ARMAR COMPRA 9.4")
            logging.debug(f"    MME9: -3 = {mme.iloc[-3]:.7f}, -2 = {mme.iloc[-2]:.7f}, -1 = {mme.iloc[-1]:.7f}")
            logging.debug(f"    Gatilho = high[-1] = {gatilho:.7f}")

            return {
            'status': 'ARMAR COMPRA 9.4',
            'gatilho': gatilho,
            'tipo': 'compra',
            'coluna': 'high'
            }
        logging.debug(f"    ----------------------------------------------")
        logging.debug(f"    Curva S estabelecida")
        logging.debug(f"    MME9[-3]: {mme.iloc[-3]} > MME9[-2]: {mme.iloc[-2]} < MME9[-1]: {mme.iloc[-1]}")
        logging.debug(f"    Mínima do candle [-2] ROMPIDA pelo candle [-1]")
        logging.debug(f"    Candle [-1] (c1): low={c1['low']} < candle [-2]: (c2) low={c2['low']}")
        logging.debug(f"    ----------------------------------------------")

    # === VENDA
    if (tendencia_suave(mme, tipo='baixa', passo=PASSO_TENDENCIA_SUAVE, periodo=PERIODOS_TENDENCIA_SUAVE) and
        mme.iloc[-3] < mme.iloc[-2] and
        mme.iloc[-2] > mme.iloc[-1]):

        logging.debug(f"[{ativo}] 🔼 Tendência de baixa e S confirmados")
        c1 = df.iloc[-1]
        c2 = df.iloc[-2]
        c3 = df.iloc[-3]

        if (c2['high'] > c1['high']):

            logging.debug(f"    Candle [-1] (c1): close={c1['close']}, low={c1['low']}, high={c1['high']}")
            logging.debug(f"    Candle [-2] (c2): close={c2['close']}, low={c2['low']}")
            logging.debug(f"    Candle [-3] (c3): close={c3['close']}, low={c3['low']}")
            logging.debug(f"    Candle [0] (zero): low={candle_zero['low']}, high={candle_zero['high']}")

            logging.debug(f"    ----------------------------------------------")
            logging.debug(f"    Curva S estabelecida")
            logging.debug(f"    MME9[-3]: {mme.iloc[-3]} < MME9[-2]: {mme.iloc[-2]} > MME9[-1]: {mme.iloc[-1]}")
            logging.debug(f"    Máxima do candle [-2] NÃO rompida pelo candle [-1]")
            logging.debug(f"    Candle [-1] (c1): high={c1['high']} > candle [-2]: (c2) high={c2['high']}")
            logging.debug(f"    ----------------------------------------------")

            gatilho = c1['low']
            if candle_zero['low'] < c1['low']:
                logging.debug(f"✅ DISPARAR VENDA 9.4")
                logging.debug(f"    Gatilho = low[-1] = {gatilho:.7f}")

                return {
                'status': 'DISPARAR VENDA 9.4',
                'gatilho': gatilho,
                'tipo': 'venda',
                'coluna': 'low'
                }
                
            logging.debug(f"✅ ARMAR VENDA 9.4")
            logging.debug(f"    MME9: -3 = {mme.iloc[-3]:.7f}, -2 = {mme.iloc[-2]:.7f}, -1 = {mme.iloc[-1]:.7f}")
            logging.debug(f"    Gatilho = high[-1] = {gatilho:.7f}")

            return {
            'status': 'ARMAR VENDA 9.4',
            'gatilho': gatilho,
            'tipo': 'venda',
            'coluna': 'low'
            }

        logging.debug(f"    ----------------------------------------------")
        logging.debug(f"    Curva S estabelecida")
        logging.debug(f"    MME9[-3]: {mme.iloc[-3]} < MME9[-2]: {mme.iloc[-2]} > MME9[-1]: {mme.iloc[-1]}")
        logging.debug(f"    Máxima do candle [-2] ROMPIDA pelo candle [-1]")
        logging.debug(f"    Candle [-1] (c1): high={c1['high']} < candle [-2]: (c2) high={c2['high']}")
        logging.debug(f"    ----------------------------------------------")

    return None

# === Setups de Alexandre Wolwacz ===
# === SETUP PC (Ponto Contínuo) ===
def setup_pc(df, ativo=""):
    df = df.copy()
    candle_zero = df.iloc[-1]  # Candle [0], ainda em formação
    df = df.iloc[:-1]          # Remove o candle [0]; agora o último é o [-1]

    df['MMA21'] = df['close'].rolling(window=21).mean()
    mma = df['MMA21']

    tendencia_alta = tendencia_suave(mma, tipo='alta', passo=PASSO_TENDENCIA_SUAVE, periodo=PERIODOS_TENDENCIA_SUAVE)
    tendencia_baixa = tendencia_suave(mma, tipo='baixa', passo=PASSO_TENDENCIA_SUAVE, periodo=PERIODOS_TENDENCIA_SUAVE)

    logging.debug(f"    ANALISANDO PC")         

    logging.debug(f"🔁 MMA21 subindo? {tendencia_alta}")
    logging.debug(f"🔁 MMA21 descendo? {tendencia_baixa}")

    # === COMPRA ===
    if tendencia_alta:
        candle_menos1 = df.iloc[-1]
        media_menos1 = mma.iloc[-1]
        logging.debug(f"    Candle[-1]: open={candle_menos1['open']}, high={candle_menos1['high']}, low={candle_menos1['low']}, close={candle_menos1['close']}, MMA21={media_menos1}")

        if candle_menos1['close'] < candle_menos1['open'] and candle_menos1['open'] > media_menos1 and candle_menos1['low'] <= media_menos1: # Candle de Baixa e Abre acima da MMA21 e Mínima de [-1] Rompe a MMA21
            if candle_zero['high'] > candle_menos1['high']:
                logging.debug(f"    Disparo direto no candle[0] rompendo o gatilho do candle[-1]")
                return {
                    'status': 'DISPARAR COMPRA PC',
                    'gatilho': candle_menos1['high'],
                    'tipo': 'compra',
                    'coluna': 'high',
                    'debug_origem': 'COMPRA-DISPARO-DIRETO'
                }
            logging.debug(f"    Candle [-1] de baixa tocando MMA21. Armar gatilho: high={candle_menos1['high']}")
            return {
                'status': 'ARMAR COMPRA PC',
                'gatilho': candle_menos1['high'],
                'tipo': 'compra',
                'coluna': 'high',
                'debug_origem': 'COMPRA-ARMAR'
            }
        print ("-" * 92)
        logging.debug(f"    Iniciando avaliação de escorregamento")

        candle_m1 = df.iloc[-1]
        candle_m2 = df.iloc[-2]
        if candle_m2['high'] > candle_m1['high']:
            for i in range(-2, -7, -1):
                if abs(i) > len(df):
                    break
                candle = df.iloc[i]
                media = mma.iloc[i]
                candle_i_menos1 = df.iloc[i-1]

                logging.debug(f"🔍 i={i} | open={candle['open']} close={candle['close']} low={candle['low']} high={candle['high']} MMA21={media}")
                if candle['close'] < candle['open'] and candle['open'] > media and candle['low'] <= media:
                    if candle_zero['high'] > candle_menos1['high']:
                        logging.debug(f"    Gatilho escorregado do candle {i}. Disparo confirmado no candle[0]")
                        return {
                            'status': 'DISPARAR COMPRA PC',
                            'gatilho': candle_menos1['high'],
                            'tipo': 'compra',
                            'coluna': 'high',
                            'debug_origem': f'COMPRA-DISPARO-i{i}'
                        }
                
                    logging.debug(f"    Gatilho escorregado do candle {i}. Ainda aguardando disparo.")
                    return {
                        'status': 'ARMAR COMPRA PC',
                        'gatilho': candle_menos1['high'],
                        'tipo': 'compra',
                        'coluna': 'high',
                        'debug_origem': f'COMPRA-ARMAR-i{i}'
                    }

                if candle_i_menos1['high'] < candle['high']:
                    logging.debug(f"    Gatilho NÃO escorregado, => máxima do candle[{i}]={candle['high']} é MAIOR do que a máxima do candle[{i-1}]={candle_i_menos1['high']}.")
                    return None
        
        logging.debug(f"    Gatilho NÃO escorregado => máxima do candle[-1]={candle_m1['high']} é MAIOR que máxima do candle[-2]={candle_m2['high']}.")
            
    # === VENDA ===
    if tendencia_baixa:
        candle_menos1 = df.iloc[-1]
        media_menos1 = mma.iloc[-1]
        logging.debug(f"    Candle[-1]: open={candle_menos1['open']}, high={candle_menos1['high']}, low={candle_menos1['low']}, close={candle_menos1['close']}, MMA21={media_menos1}")

        if candle_menos1['close'] > candle_menos1['open'] and candle_menos1['open'] < media_menos1 and candle_menos1['high'] >= media_menos1:
            if candle_zero['low'] < candle_menos1['low'] and candle_zero['close'] < candle_zero['open']:
                logging.debug(f"    Disparo direto no candle[0] rompendo o gatilho do candle[-1]")
                return {
                    'status': 'DISPARAR VENDA PC',
                    'gatilho': candle_menos1['low'],
                    'tipo': 'venda',
                    'coluna': 'low',
                    'debug_origem': 'VENDA-DISPARO-DIRETO'
                }
            logging.debug(f"    Candle[-1] de alta tocando MMA21. Armar gatilho: low={candle_menos1['low']}")
            return {
                'status': 'ARMAR VENDA PC',
                'gatilho': candle_menos1['low'],
                'tipo': 'venda',
                'coluna': 'low',
                'debug_origem': 'VENDA-ARMAR'
            }

        print ("-" * 92)            
        logging.debug(f"    Iniciando avaliação de escorregamento")

        candle_m1 = df.iloc[-1]
        candle_m2 = df.iloc[-2]

        if candle_m2['low'] < candle_m1['low']:
            for i in range(-2, -7, -1):
                if abs(i) > len(df):
                    break
                candle = df.iloc[i]
                media = mma.iloc[i]
                candle_i_menos1 = df.iloc[i-1]

                logging.debug(f"🔍 i={i} | open={candle['open']} close={candle['close']} low={candle['low']} high={candle['high']} MMA21={media}")
                if candle['close'] > candle['open'] and candle['open'] < media and candle['high'] >= media:
                    if candle_zero['low'] < candle_menos1['low']:
                        logging.debug(f"    Gatilho escorregado do candle {i}. Disparo confirmado no candle[0]")
                        return {
                            'status': 'DISPARAR VENDA PC',
                            'gatilho': candle_menos1['low'],
                            'tipo': 'venda',
                            'coluna': 'low',
                            'debug_origem': f'VENDA-DISPARO-i{i}'
                        }
                        logging.debug(f"    Gatilho escorregado do candle {i}. Ainda aguardando disparo.")
                    return {
                        'status': 'ARMAR VENDA PC',
                        'gatilho': candle_menos1['low'],
                        'tipo': 'venda',
                        'coluna': 'low',
                        'debug_origem': f'VENDA-ARMAR-i{i}'
                    }

                if candle_i_menos1['low'] > candle['low']:
                    logging.debug(f"    Gatilho NÃO escorregado, => mínima do candle[{i}]={candle['low']} é MENOR do que a mínima do candle[{i-1}]={candle_i_menos1['low']}.")
                    return None

        logging.debug(f"    NÃO escorregado => mínima do candle[-1]={candle_m1['low']} é MENOR que mínima do candle[-2]={candle_m2['low']}.")

    return None

# === EXECUÇÃO PRINCIPAL ===
if __name__ == "__main__":
    if not dentro_do_horario():
        logging.info("Fora do horário configurado. Encerrando execução.")
        exit()

    try:
        ativos_df = pd.read_excel(ARQUIVO_EXCEL)
    except Exception as e:
        logging.error(f"Erro ao carregar o arquivo Excel: {e}")
        exit()

    # Remove colunas duplicadas (mantém apenas a primeira ocorrência)
    ativos_df = ativos_df.loc[:, ~ativos_df.columns.duplicated(keep='first')]


    logging.info(f"Execução iniciada em {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    logging.info('*' * 92)

    # Criando dicionário candles_dict
    candles_dict = {}

    for idx, ativo in ativos_df.iterrows():
        par = ativo['Par']
        timeframe = str(ativo['Timeframe'])
        mercado = ativo.get('Mercado', 'linear')

        try:
            df = obter_candles(par=par, interval=timeframe, mercado=mercado)
            candles_dict[par] = df.copy()

            # Calcula médias móveis com candle [0]
            df['MME9'] = df['close'].ewm(span=9).mean()
            df['MMA21'] = df['close'].rolling(window=21).mean()

            # Armazena para gráficos, com índice resetado
            candles_dict[par] = df.reset_index(drop=True)

        except Exception as e:
            logging.error(f"Erro ao obter dados de {par}: {e}")
            ativos_df.at[idx, 'Último Setup Identificado'] = f"Erro: {e}"
            continue

        if df.empty or len(df) < PERIODOS_MINIMO:
            logging.warning(f"Poucos dados para {par} ({timeframe}min)")
            ativos_df.at[idx, 'Último Setup Identificado'] = "Sem dados"
            continue

        # 🔍 Exibe os candles mais recentes com MME9 e MMA21 apenas uma vez
        df['MME9'] = df['close'].ewm(span=9).mean()
        df['MMA21'] = df['close'].rolling(window=21).mean()

        logging.info(f"\n🔍 Verificando {par} ({timeframe} min - {mercado})")

        # 🔍 Verifica e exibe os candles [-10] a [0]
        logging.info("  🔍 Verificando integridade dos candles [-10] a [0]:")

        for i in range(-10, 0):
            candle = df.iloc[i]
            mme9 = df['MME9'].iloc[i] if 'MME9' in df.columns else None
            mma21 = df['MMA21'].iloc[i] if 'MMA21' in df.columns else None

            logging.info(
                f"     idx={i:>2} | time={candle['timestamp']} | "
                f"o={candle['open']:.7f} h={candle['high']:.7f} l={candle['low']:.7f} c={candle['close']:.7f} "
                f"| m9={mme9:.7f} | m21={mma21:.7f}"
            )
            # Adiciona ao CSV
            dados_integridade.append({
                'Par': par,
                'Timeframe': timeframe,
                'Mercado': mercado,
                'idx': i,
                'timestamp': candle['timestamp'],
                'open': candle['open'],
                'high': candle['high'],
                'low': candle['low'],
                'close': candle['close'],
                'MME9': mme9,
                'MMA21': mma21,
                'Setup Identificado': ''   # Preenchido depois se setup for detectado
            })
        print("=" * 92)

        resultado_final = None
        status_do_setup = "Nenhum"

        for func in [setup_9_1, setup_9_2, setup_9_3, setup_9_4, setup_pc]:
            resultado = func(df, ativo=par)
            logging.debug(f"  ▶️Resultado de {func.__name__}: {resultado}")
            print("=" * 92)
            
            # BLOCO DE DISPARO
            if resultado and isinstance(resultado, dict) and resultado['status'].startswith("ARMAR"):
                tipo = resultado['tipo']
                coluna = resultado['coluna']
                gatilho = resultado['gatilho']
                preco_atual = df[coluna].iloc[0]

                if DEBUG_MODE:
                    logging.debug(f"  📍 idx=0 | Preço atual ({coluna}[0]) = {preco_atual:.7f}")
                    logging.debug(f"  🔎 Gatilho atual = {gatilho:.7f}")

                rompeu = (tipo == 'compra' and preco_atual > gatilho) or (tipo == 'venda' and preco_atual < gatilho)

                if rompeu:
                    logging.debug("  ✅ DISPARO no candle [0] confirmado!")
                    resultado['status'] = resultado['status'].replace("ARMAR", "DISPARAR")

            # CONSTRÓI O RESULTADO FINAL
            if resultado is not None and isinstance(resultado, dict):
                precisao = f".{CASAS_DECIMAIS_GATILHO}f"
                gatilho_formatado = format(resultado['gatilho'], precisao)
                candle_m1 = df.iloc[-1]
                high_m1 = candle_m1['high']
                low_m1 = candle_m1['low']

                resultado_final = (
                    f"{resultado['status']} (gatilho: {gatilho_formatado} | "
                    f"h: {high_m1:.7f} | l: {low_m1:.7f}) "
                    f"({df['timestamp'].iloc[-1].strftime('%d/%m/%Y %H:%M:%S')})"
                )
                status_do_setup = resultado['status']
                break  # <- Apenas o primeiro setup encontrado será reportado

        # Atualiza a última linha do CSV com o nome do setup
        if len(dados_integridade) >= 6:
            dados_integridade[-6:][-1]['Setup Identificado'] = status_do_setup

        if resultado_final is None:
            resultado_final = f"Nenhum ({df['timestamp'].iloc[-1].strftime('%d/%m/%Y %H:%M:%S')})"

        logging.info(f" >>>> {par}: {resultado_final}")
        ativos_df.at[idx, 'Último Setup Identificado'] = resultado_final

        # Envio de alerta apenas para ARMAR ou DISPARAR
        if status_do_setup.startswith("ARMAR") or status_do_setup.startswith("DISPARAR"):
            mensagem = f"🚨 {par} | {resultado_final}"
            enviar_alerta_telegram(mensagem) # Cancelar para evitar muita informação no debgu
        print("+" * 92)

    # === SALVAMENTO E ENCERRAMENTO FINAL ===
    # Salva a planilha Excel original

    ARQUIVO_EXCEL_OPT = "ativos_opt_hr.xlsx"

    try:
        gerar_excel_com_graficos(candles_dict, ativos_df, nome_arquivo=ARQUIVO_EXCEL_OPT)
        logging.info(f"✅ Arquivo '{ARQUIVO_EXCEL_OPT}' salvo com sucesso com dados e gráficos.")
    except Exception as e:
        logging.error(f"❌ Erro ao gerar o arquivo '{ARQUIVO_EXCEL_OPT}': {e}")

    # Exporta os dados de integridade para CSV
    try:
        df_csv = pd.DataFrame(dados_integridade)
        df_csv.to_csv("dados_candles.csv", index=False)
        logging.info("📁 Arquivo 'dados_candles.csv' salvo com os candles mais recentes.")
    except Exception as e:
        logging.error(f"❌ Erro ao salvar CSV: {e}")


    #Chamada da função para envio ao Google Drive no Hostinger
    upload_file_to_drive('ativos_opt_hr.xlsx', os.environ.get("GDRIVE_FOLDER_ID")) #Grava sempre o mesmo arquivo ativos_opt_hr.xlsx

    logging.info("-" * 60)
    logging.info(f"🏁 Execução finalizada em {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")

    # (coloque isso no final do script)
    for handler in logging.getLogger().handlers:
    handler.flush()
    handler.close()

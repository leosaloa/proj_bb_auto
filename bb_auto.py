import pandas as pd
import os
import datetime
import string

# Mapeamento pasta e importação do arquivo
origem = r'C://projetos//projeto//processar//'
destino = r'C://projetos//projeto//processado//'

if not os.path.exists(origem):
    os.makedirs(origem)
    print('PASTA ORIGEM CRIADA')
if not os.path.exists(destino):
    os.makedirs(destino)
    print('PASTA DESTINO CRIADA')

dt_atual = datetime.datetime.now()
arquivos = os.listdir(origem)
arquivo_auto = pd.read_excel(f'{origem}{arquivos[0]}', header=2)
df_origi = pd.DataFrame(arquivo_auto)
df = df_origi

# Ajustar para remover nomes incorretos
todos_caracteres = string.printable
alfabeto = string.ascii_letters
#nao_alfabeto = ''.join(char for char in todos_caracteres if )#####

# Trata base
df['CADASTRO'] = df['CADASTRO'].astype(str)
df['DT_EMISSAO'] = df['DT_EMISSAO'].dt.strftime('%d/%m/%Y')
df['VIGENCIA_INICIO'] = df['VIGENCIA_INICIO'].dt.strftime('%d/%m/%Y')
df['VIGENCIA_FIM'] = df['VIGENCIA_FIM'].dt.strftime('%d/%m/%Y')
df['VL_PARCELA'] = df['VL_PARCELA'].apply(lambda x: f'{x:.2f}'.replace('.', ','))
df['VL_PARCELA'] = df['VL_PARCELA'].astype(str)
df['DDD'] = df['DDD DO TOMADOR'].fillna(0).astype(int).astype(str)
df['DDD'] = df['DDD'].replace('0', '')
df['TELEFONE COMPLETO'] = df['TELEFONE'].fillna(0).astype('Int64').astype(str)
df['TELEFONE COMPLETO'] = '55' + df['DDD'] + df['TELEFONE COMPLETO']
df['NUM_CARACT'] = df.apply(lambda x: len(x['TELEFONE COMPLETO']), axis=1)
df['ATRASO'] = (dt_atual - df['VENCTO_PARCELA']).dt.days
df['VENCTO_PARCELA'] = df['VENCTO_PARCELA'].dt.strftime('%d/%m/%Y')
df['DOC_TOMADOR'] = df['DOC_TOMADOR'].astype(str)
df['DOC_TOMADOR2'] = df['DOC_TOMADOR']
df['NOME'] = df.apply(lambda x: x['NOME_TOMADOR'].split(' ')[0], axis=1)
df['QTD_TOTAL_PARCELAS'] = df['QTD_TOTAL_PARCELAS'].fillna(0).astype(int).astype(str)
df['QTD_TOTAL_PARCELAS'] = df['QTD_TOTAL_PARCELAS'].replace('0', '')
df['DDD DO TOMADOR'] = df['DDD DO TOMADOR'].fillna(0).astype(int).astype(str)
df['TELEFONE'] = df['TELEFONE'].fillna(0).astype(int).astype(str)
# Fim tratamento da base

# DDD RIO GRANDE DO SUL
df['DDD_SUL'] = ((df['DDD'] == '51')
                 | (df['DDD'] == '53')
                 | (df['DDD'] == '54')
                 | (df['DDD'] == '55')
                 )

df_projeto = df
df_bot_bb = df

# Filtrar base projeto
df_projeto = df_projeto[(df_projeto['PARCELA'] != 1)
                        & (df_projeto['SITUACAO'] == 'RE')
                        & (df_projeto['TIPO'] == 'DB')
                        & ((df_projeto['ATRASO'] == 2) | (df['ATRASO'] == 6))
                        & (df_projeto['NUM_CARACT'] == 13)
                        #& (df_projeto['DDD_SUL'] == False)  # RETIRAR ASSIM QUE VOLTAR A COBRAR O RS
                        ]

# Layout do arquivo projeto
colunas_projeto = {
    'DOC_TOMADOR': 'ID',
    'TELEFONE COMPLETO': 'TELEFONE',
    'NOME_TOMADOR': 'NOME',
    'DOC_TOMADOR2': 'CPF',
    'NOME': 'nome_cliente',
    'CADASTRO': 'CADASTRO',
    'ENDOSSO': 'endosso',
    'PARCELA': 'numero_parcela',
    'VENCTO_PARCELA': 'vencimento',
    'VL_PARCELA': 'valor',
}

# -----------------------------------------------------------------------#
# TRATATIVAS PARA O ARQUIVO BOT_BB
df_bot_bb['Coluna1'] = None
df_bot_bb['Coluna2'] = None
df_bot_bb['Coluna3'] = None
df_bot_bb['VENCTO_PARCELA89'] = None
df_bot_bb['VENCTO_PARCELA882'] = None
df_bot_bb['VENCTO_PARCELA883'] = None

df_bot_bb = df_bot_bb[(df_bot_bb['PARCELA'] != 1)
                      & (df_bot_bb['SITUACAO'] == 'RE')
                      & (df_bot_bb['TIPO'] == 'DB')
                      & (df_bot_bb['ATRASO'] >= 4)
                      & (df_bot_bb['ATRASO'] <= 15)
                      & (df_bot_bb['NUM_CARACT'] == 13)
                      #& (df_bot_bb['DDD_SUL'] == False)  # RETIRAR ASSIM QUE VOLTAR A COBRAR O RS
                      ]

colunas_bot_bb = {'RAMO': 'RAMO',
                  'PRODUTO': 'PRODUTO',
                  'CADASTRO': 'CADASTRO',
                  'ENDOSSO': 'ENDOSSO',
                  'PROVISORIO': 'PROVISORIO',
                  'Coluna3': 'Coluna3',
                  'Coluna2': 'Coluna2',
                  'Coluna1': 'Coluna1',
                  'PARCELA': 'PARCELA',
                  'QTD_TOTAL_PARCELAS': 'QTD_TOTAL_PARCELAS',
                  'SITUACAO': 'SITUACAO',
                  'TIPO': 'TIPO',
                  'TIPO_EMISS020': 'TIPO_EMISS020',
                  'LINHA_DIGI': 'LINHA_DIGI',
                  'LINHA_DIGI_MCC': 'LINHA_DIGI_MCC',
                  'NN1': 'NN12',
                  'DT_EMISSAO': 'DT_EMISSAO',
                  'USER_EMISSAO': 'USER_EMISSAO',
                  'VIGENCIA_INICIO': 'VIGENCIA_INICIO',
                  'VIGENCIA_FIM': 'VIGENCIA_FIM',
                  'VL_PARCELA': 'VL_PARCELA',
                  'VENCTO_PARCELA': 'VENCTO_PARCELA',
                  'VENCTO_PARCELA88': 'VENCTO_PARCELA88',
                  'VENCTO_PARCELA883': 'VENCTO_PARCELA883',
                  'VENCTO_PARCELA882': 'VENCTO_PARCELA882',
                  'VENCTO_PARCELA89': 'VENCTO_PARCELA89',
                  'CORRETOR': 'CORRETOR',
                  'NOME_CORRETOR': 'NOME_CORRETOR',
                  'DDD DO CORRETOR': 'DDD DO CORRETOR',
                  'TELEFONE DO CORRETOR': 'TELEFONE DO CORRETOR',
                  'E-MAIL DO CORRETOR': 'E-MAIL DO CORRETOR',
                  'TIP_DOC_TOMADOR': 'TIP_DOC_TOMADOR',
                  'DOC_TOMADOR': 'DOC_TOMADOR',
                  'NOME_TOMADOR': 'NOME_TOMADOR',
                  'DDD DO TOMADOR': 'DDD DO TOMADOR',
                  'TELEFONE': 'TELEFONE',
                  'E-MAIL1 DO TOMADOR': 'E-MAIL1 DO TOMADOR',
                  'QTD_REPROGRAMACOES': 'QTD_REPROGRAMACOES',
                  'DT_VENCTO_REPROG': 'DT_VENCTO_REPROG',
                  'Soma de VL_REPROG': 'Soma de VL_REPROG',
                  'NN_REPROG': 'NN_REPROG',
                  'TIPO_REPROG': 'TIPO_REPROG',
                  'LINHA_DIGITAVEL_REPROG': 'LINHA_DIGITAVEL_REPROG',
                  'COD_PLANO_PAGAMENTO': 'COD_PLANO_PAGAMENTO',
                  'PLANO_PAGAMENTO': 'PLANO_PAGAMENTO',
                  'CANAL_DISTRIBUIDOR': 'CANAL_DISTRIBUIDOR',
                  'DEBITO_ENVIADO_AO_BANCO': 'DEBITO_ENVIADO_AO_BANCO',
                  'BANCO': 'BANCO',
                  'CD_RET_BANCO': 'CD_RET_BANCO',
                  'RETORNO_BANCARIO': 'RETORNO_BANCARIO',
                  'QTD_DIAS_EM_ABERTO': 'QTD_DIAS_EM_ABERTO',
                  'TIPOLOGIA_ENDOSSO': 'TIPOLOGIA_ENDOSSO',
                  'QTD_REPIQUES': 'QTD_REPIQUES'
                  }

#---------------------------------------------------#

##### EXTRAÇÃO #####
# projeto
df_projeto = df_projeto.rename(columns=colunas_projeto)[list(colunas_projeto.values())]
# 10% DA BASE
df_projeto_10 = df_projeto.sample(frac=0.1, random_state=1) #
df_projeto_10.to_excel(f'{destino}AUTO_BB_DB_D2_6_{arquivos[0].split("_")[4]}', index=False) #

# BASE COMPLETA
#df_projeto.to_excel(f'{destino}AUTO_BB_DB_D2_6_{arquivos[0].split("_")[4]}', index=False)
print('Extração projeto FINALIZADA!')

# BOT_projeto
df_bot_bb = df_bot_bb.rename(columns=colunas_bot_bb)[list(colunas_bot_bb.values())]
# 10% DA BASE
df_bot_bb_10 = df_bot_bb.sample(frac=0.1, random_state=1)#
df_bot_bb_10.to_csv(f'{destino}BASE BB - {arquivos[0].split("_")[4].replace("xlsx", "csv")}', sep=';', index=False)#

# BASE COMPLETA
#df_bot_bb.to_csv(f'{destino}BASE BB - {arquivos[0].split("_")[4].replace("xlsx", "csv")}', sep=';', index=False)
print('Extração BOT_projeto FINALIZADA!')
dt_fim = datetime.datetime.now()
print(f'Tempo de execução: {dt_fim - dt_atual}')

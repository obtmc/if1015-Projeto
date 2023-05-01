import pandas as pd
from sklearn.preprocessing import MinMaxScaler, StandardScaler

# Carrega o dataset
df = pd.read_csv('sedec_solicitacoes.csv')
print(df)
df = df.sort_values('solicitacao_data', ascending=True)
print(df)

# Remove as colunas especificadas
df = df.drop(['mes', 'ano', 'solicitacao_regional', 'solicitacao_localidade', 'rpa_nome', 'solicitacao_microrregiao', 'solicitacao_roteiro', 'solicitacao_plantao', 'processo_situacao', 'processo_tipo', 'processo_origem', 'processo_solicitacao'], axis=1)

# Reune as informações de data e hora em uma ínica coluna e depois organiza aas linhas pelo valor ascendente dessas novas informações
df['data_hora'] = pd.to_datetime(df['solicitacao_data'] + ' ' + df['solicitacao_hora'])
df = df.sort_values('data_hora', ascending=True)

df = df.rename(columns={'solicitacao_data': 'solicitacao_data_hora'})
df = df.drop(['solicitacao_hora'], axis=1)

# Excluir as linhas em que a coluna "rpa_codigo" tenha o valor "Não informada"
df = df[df['rpa_codigo'] != 'Não informada']

# Calcular a diferença entre as linhas na coluna "data_hora"
#df['data_hora'] = df['data_hora'] - df['data_hora'].shift(1)

# Preencher a primeira linha com 0
#df.iloc[0, df.columns.get_loc('data_hora')] = '0 days 00:00:00'

df = df.rename(columns={'data_hora': 'delay'})

# Normaliza os dados da coluna especificada usando MinMaxScaler
scaler = MinMaxScaler(feature_range=(0, 1))
df['delay'] = scaler.fit_transform(df[['delay']])

df['delay'] = df['delay']*100000

#df = df.sort_values('delay', ascending=True)
print(df['delay'])

# Salva o dataset resultante em um novo arquivo CSV
df.to_csv('dataset_tratado.csv', index=False)
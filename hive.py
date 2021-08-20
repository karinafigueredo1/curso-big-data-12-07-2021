from pyhive import hive
from datetime import datetime
import pandas as pd
import MySQLdb



def getConnHive():
    
    #local
    connHive = hive.Connection(host='ffborelli.ddns.net', 
    port=10000, 
        username='hive', password = 'hive', auth = 'CUSTOM')
    return connHive

#def main(args):
    
#print('a')
connHive = getConnHive()
#print(connHive)

#dfHive = pd.read_sql("SELECT * FROM big_data.karina_figueredo limit 10", connHive)
    
#print (datetime.now().strftime('%Y-%m-%d %H:%M:%S') + " --- {} Registros Consultados do Apache Hive ".format(dfHive.size) )

#query = 'INSERT INTO big_data.karina_figueredo ( `date`, volume, `open`, high, low, `close`, adjclose ) VAlUES ("2021-08-03", 47,25,31,22,30,30.55)'

#cur = connHive.cursor()
#cur.execute(query)
#connHive.commit()

# SEGUNDO EXERCICIO - Primeiro passo leitura da tabela


#pegar os dados da tabela stock_opcoes
#dfStockOpcoes = pd.read_sql("SELECT * FROM big_data.stock_opcoes limit 1", connHive)

#print( dfStockOpcoes )

# processar os dados -> dobrar os valores das colunas, com excecao da data
#for row in dfStockOpcoes.iterrows():
    #print(row[1]['stock_opcoes.volume'])
    #print(row[1]['stock_opcoes.data'])
    #ins = "INSERT INTO big_data.karina_figueredo VALUES ('"+row[1]['stock_opcoes.data']+"', " + str(2 * row[1]['stock_opcoes.volume'])  + " , " + str(2 * row[1]['stock_opcoes.abertura']) + ", " + str(2* row[1]['stock_opcoes.maxima'])+ ", " +str( 2 *row[1]['stock_opcoes.minima'])+ " , " + str (2*  row[1]['stock_opcoes.fechamento'])+ " , " +str(2 *row[1]['stock_opcoes.ajuste'])+ ")" 
    #print (ins)
    #cur = connHive.cursor()
    #cur.execute(ins)
    #connHive.commit()

#Exercicio 3 - Passo 1 - selecionar todos os dados da tabela do aluno (karina.figueredo)


#aluno = 'karina_figueredo'
#vol_min = 



#for row in dfAluno.iterrows():
#    menor = min(dfAluno)
#    maior = max(dfAluno)
#    print (menor)
#    print(maior)
    #print(row[1]['stock_opcoes.volume'])
    #print(row[1]['stock_opcoes.data'])
    #insertnovo = "INSERT INTO big_data.estatisticas VALUES ('"+row[1]['estatisticas.data']+"', " + str(2 * row[1]['stock_opcoes.volume'])  + " , " + str(2 * row[1]['stock_opcoes.abertura']) + ", " + str(2* row[1]['stock_opcoes.maxima'])+ ", " +str( 2 *row[1]['stock_opcoes.minima'])+ " , " + str (2*  row[1]['stock_opcoes.fechamento'])+ " , " +str(2 *row[1]['stock_opcoes.ajuste'])+ ")" 
    #print (ins)
    #cur = connHive.cursor()
    #cur.execute(ins)
    #connHive.commit()
dfAluno = pd.read_sql("SELECT * FROM big_data.karina_figueredo", connHive)
#vol_min = dfAluno['karina_figueredo.volume'][0]
#vol_max = dfAluno['karina_figueredo.volume'][0]
#abert_min = dfAluno['karina_figueredo.open'][0]
#abert_max = dfAluno['karina_figueredo.open'][0]
#fecha_min = dfAluno['karina_figueredo.close'][0]
#fecha_max = dfAluno['karina_figueredo.close'][0]
#for row in dfAluno.iterrows():
#    if (row [1]['karina_figueredo.volume'] < vol_min):
#        vol_min = row[1]['karina_figueredo.volume']
#    if (row [1]['karina_figueredo.open'] < abert_min):
#        abert_min = row[1]['karina_figueredo.open']
#    if (row [1]['karina_figueredo.close'] < fecha_min):
#        fecha_min = row[1]['karina_figueredo.close']
#    if (row [1])['karina_figueredo.volume'] > vol_max:
#        vol_max = row [1]['karina_figueredo.volume']
#    if (row [1])['karina_figueredo.open'] > abert_max:
#        abert_max = row [1]['karina_figueredo.open']
#    if (row [1])['karina_figueredo.close'] > fecha_max:
#        fecha_max = row [1]['karina_figueredo.close']
#query = "INSERT INTO big_data.estatisticas VALUES ('Karina Figueredo', {vol_min}, {vol_max}, {abert_min}, {abert_max}, {fecha_min}, {fecha_max})".format(vol_min=vol_min, vol_max=vol_max, abert_min=abert_min, abert_max=abert_max, fecha_min=fecha_min, fecha_max=fecha_max)
# print(query)
# cur = connHive.cursor()
# cur.execute(query)
# connHive.commit()
# dfUserData = pd.read_sql ( "SELECT * FROM big_data.karina_figueredo limit 10", connHive )
# print(dfUserData)

# vol_min = str ( dfUserData['karina_figueredo.volume'].min() )
# vol_max = str ( dfUserData['karina_figueredo.volume'].max() )
# abert_min = str ( dfUserData['karina_figueredo.open'].min() )
# abert_max = str ( dfUserData['karina_figueredo.open'].max() )
# fecha_min = str ( dfUserData['karina_figueredo.close'].min() ) 
# fecha_max = str ( dfUserData['karina_figueredo.close'].max() )
# aluno = 'Karina Figueredo'

# insert = "INSERT INTO big_data.estatisticas VALUES ( '{aluno}', {vol_min}, {vol_max}, {abert_min}, {abert_max}, {fecha_min}, {fecha_max} )".format( fecha_max=fecha_max, fecha_min=fecha_min, vol_min=vol_min, vol_max=vol_max, abert_max=abert_max, abert_min=abert_min, aluno=aluno )
# print (insert)
# cur = connHive.cursor()
# cur.execute(insert)
# connHive.commit()


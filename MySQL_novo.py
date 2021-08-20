import pandas as pd
import MySQLdb

def getConnMySQL():
    print(__name__)
    connMySQL = MySQLdb.connect(
        host='ffborelli.ddns.net',
        user='root',
        passwd='root',
        db='karina_figueredo'
    )

    return connMySQL
connMySQL = getConnMySQL()

csvclientes = pd.read_csv("/home/virtual/Downloads/clientes.csv", header = None)
csvlocacao = pd.read_csv("/home/virtual/Downloads/locacao.csv", header = None)
csvveiculos = pd.read_csv("/home/virtual/Downloads/veiculos.csv", header = None)
csvdespachantes = pd.read_csv("/home/virtual/Downloads/despachantes.csv",header = None )

# for row in csvclientes.iterrows():
#     id_clientes = str(row[1][0])
#     CPF = str(row[1][1])
#     CNH = str(row[1][2])
#     validade_CNH = str(row[1][3])
#     nome = str(row[1][4])
#     data_cadastro = str(row[1][5])
#     data_nascimento = str(row[1][6])
#     telefone = str(row[1][7])
#     status = str(row[1][8])
    
#     query = """
#         INSERT INTO karina_figueredo.Clientes
#         (id_clientes, CPF, CNH, validade_CNH, nome, data_cadastro, data_nascimento, telefone, status)
#         VALUES
#         ({id_clientes}, {CPF}, {CNH}, {validade_CNH}, '{nome}', '{data_cadastro}', '{data_nascimento}', {telefone}, '{status}')
#         """.format(id_clientes=id_clientes,CPF=CPF,CNH=CNH,validade_CNH=validade_CNH,nome=nome,data_cadastro=data_cadastro,data_nascimento=data_nascimento, telefone=telefone, status=status)
    
    # print (query)
    # cur = connMySQL.cursor()
    # cur.execute(query)
    # connMySQL.commit()

# for row in csvveiculos.iterrows():
#     id_veiculo = str(row[1][0])
#     data_aquisicao = str(row[1][1])
#     ano = str(row[1][2])
#     modelo = str(row[1][3])
#     placa = str(row[1][4])
#     status = str(row[1][5])
#     diaria = str(row[1][6])
#     query = """
#          INSERT INTO karina_figueredo.veiculos
#          (id_veiculo, data_aquisicao, ano, modelo, placa, status, diaria)
#          VALUES
#          ({id_veiculo}, '{data_aquisicao}', {ano}, '{modelo}', '{placa}', '{status}', {diaria})
#          """.format(id_veiculo=id_veiculo,data_aquisicao=data_aquisicao,ano=ano,modelo=modelo,placa=placa,status=status,diaria=diaria)
#     print (query)
#     cur = connMySQL.cursor()
#     cur.execute(query)
#     connMySQL.commit()

# for row in csvdespachantes.iterrows():
#     id_despachantes = str(row[1][0])
#     nome = str(row[1][1])
#     status = str(row[1][2])
#     filial = str(row[1][3])
    
#     query = """
#          INSERT INTO karina_figueredo.despachantes
#          (id_despachantes, nome, status, filial)
#          VALUES
#          ({id_despachantes}, '{nome}', '{status}', '{filial}')
#          """.format(id_despachantes=id_despachantes,nome=nome, status=status,filial=filial)
#     print(query)
#     cur = connMySQL.cursor()
#     cur.execute(query)
#     connMySQL.commit()

for row in csvlocacao.iterrows():
    id_locacao = str(row[1][0])
    id_clientes = str(row[1][1])
    id_despachante = str(row[1][2])
    id_veiculo = str(row[1][3])
    data_locacao = str(row[1][4])
    data_entrega = str(row[1][5])
    total = str(row[1][6])
        
    query = """
        INSERT INTO karina_figueredo.Locacao
        (id_locacao, id_clientes, id_despachante, id_veiculo, data_locacao, data_entrega, total)
        VALUES
        ({id_locacao}, {id_clientes}, {id_despachante}, {id_veiculo}, '{data_locacao}', '{data_entrega}', {total})
        """.format(id_locacao=id_locacao, id_clientes=id_clientes, id_despachante=id_despachante,id_veiculo=id_veiculo, data_locacao=data_locacao, data_entrega=data_entrega,total=total)
    print(query)
    cur = connMySQL.cursor()
    cur.execute(query)
    connMySQL.commit()
    
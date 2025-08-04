from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, Column, Integer, String, Date, Numeric, ForeignKey, text
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import SQLAlchemyError
import requests
from datetime import date
from typing import List

app = FastAPI(
    title="API de Contratos e Títulos XPTO",
    description="Gerencie contratos de assistência funerária, titulares e títulos, incluindo verificação de inadimplência.",
    version="1.0.0"
)

DATABASE_URL = "mssql+pyodbc://ThiagoSousa01:pswThiagoSousa01@i9.server.pbr.digital:1400/dbThiagoSousa01?driver=ODBC+Driver+17+for+SQL+Server"
engine = create_engine(DATABASE_URL)
Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class Titular(Base):
    __tablename__ = 'titulares'
    id = Column(Integer, primary_key=True)
    nome = Column(String(255), nullable=False)
    cep = Column(String(9), nullable=False)
    logradouro = Column(String(255), nullable=False)

class Contrato(Base):
    __tablename__ = 'contratos'
    id = Column(Integer, primary_key=True)
    titular_id = Column(Integer, ForeignKey('titulares.id'), nullable=False)
    data_contrato = Column(Date, default=date.today())
    total_pago = Column(Numeric(10, 2), default=0.00)

class Titulo(Base):
    __tablename__ = 'titulos'
    id = Column(Integer, primary_key=True)
    contrato_id = Column(Integer, ForeignKey('contratos.id'), nullable=False)
    valor = Column(Numeric(10, 2), nullable=False)
    data_vencimento = Column(Date, nullable=False)
    data_pagamento = Column(Date, nullable=True)
    valor_pago = Column(Numeric(10, 2), nullable=True)

class TituloCreate(BaseModel):
    valor: float = Field(..., gt=0, description="Valor do título deve ser maior que zero.")
    data_vencimento: date

class ContratoCreate(BaseModel):
    nome_titular: str = Field(..., min_length=1, max_length=255, description="Nome do titular do contrato.")
    cep: str = Field(..., min_length=8, max_length=9, description="CEP do titular.")
    titulos: List[TituloCreate] = Field(..., min_items=1, description="Lista de títulos associados ao contrato.")

@app.post("/contratos", status_code=status.HTTP_201_CREATED)
async def adicionar_contrato(contrato_data: ContratoCreate):
    session = Session()
    try:
        cep_limpo = contrato_data.cep.replace('-', '')
        cep_api_url = f"https://brasilapi.com.br/api/cep/v1/{cep_limpo}"
        response = requests.get(cep_api_url)
        
        if response.status_code != 200:
            raise HTTPException(status_code=400, detail={"error": "CEP inválido ou não encontrado.", "details": response.json()})
        cep_info = response.json()
        logradouro = cep_info.get('street', 'Logradouro não informado')
        novo_titular = Titular(nome=contrato_data.nome_titular, cep=contrato_data.cep, logradouro=logradouro)
        session.add(novo_titular)
        session.flush()  
        novo_contrato = Contrato(titular_id=novo_titular.id)
        session.add(novo_contrato)
        session.flush()  
        for titulo_item in contrato_data.titulos:
            novo_titulo = Titulo(contrato_id=novo_contrato.id, valor=titulo_item.valor, data_vencimento=titulo_item.data_vencimento)
            session.add(novo_titulo)
        
        session.commit() 
        return {"message": "Contrato, titular e títulos adicionados com sucesso!", "contrato_id": novo_contrato.id}
    
    except HTTPException as e:
        session.rollback() 
        raise e
    except SQLAlchemyError as e:
        session.rollback()
        raise HTTPException(status_code=500, detail={"error": "Erro no banco de dados", "details": str(e)})
    except requests.exceptions.RequestException as e:
        session.rollback()
        raise HTTPException(status_code=500, detail={"error": "Erro ao conectar com a API de CEP", "details": str(e)})
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail={"error": "Ocorreu um erro inesperado", "details": str(e)})
    finally:
        session.close() 
        
@app.get("/contratos/status")
async def get_contratos_status():
    session = Session()
    try:
        sql_query = text("""
            SELECT
                c.id AS contrato_id,
                t.nome AS nome_titular,
                CASE
                    WHEN COUNT(CASE WHEN ti.data_vencimento < GETDATE() AND ti.data_pagamento IS NULL THEN 1 END) >= 3 THEN 'INATIVO'
                    ELSE 'ATIVO'
                END AS status_contrato,
                COALESCE(SUM(CASE WHEN ti.data_vencimento < GETDATE() AND ti.data_pagamento IS NULL THEN ti.valor ELSE 0 END), 0) AS montante_atrasado
            FROM
                contratos c
            JOIN
                titulares t ON c.titular_id = t.id
            LEFT JOIN
                titulos ti ON c.id = ti.contrato_id
            GROUP BY
                c.id, t.nome
            ORDER BY
                c.id;
        """)
        
        result = session.execute(sql_query).fetchall()
        contratos_status = []
        for row in result:
            contratos_status.append({
                "contrato_id": row.contrato_id,
                "nome_titular": row.nome_titular,
                "status_contrato": row.status_contrato,
                "montante_atrasado": float(row.montante_atrasado)
            })
        
        return contratos_status
    
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail={"error": "Erro no banco de dados", "details": str(e)})
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "Ocorreu um erro inesperado", "details": str(e)})
    finally:
        session.close() 

from sqlalchemy import create_engine, MetaData, Column, Integer, String, ForeignKey
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import os
from contextlib import contextmanager


engine = create_engine('sqlite:///data_warehouse.db', echo=False)
metadata = MetaData()
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()


class dim_parceiro(Base):
    __tablename__ = 'dim_parceiro'
    id_parceiro = Column(String, primary_key=True, unique=True)
    nome_fantasia = Column(String, nullable=False)
    telefone_parceiro = Column(String, nullable=False)
    cnpj_parceiro = Column(String, nullable=False, unique=True)

    def __repr__(self):
        return f"<dim_parceiro(nome='{self.nome_fantasia}')>"

class dim_produto(Base):
    __tablename__ = 'dim_produto'
    id_produto = Column(String, primary_key=True, unique=True)
    nome_produto = Column(String, nullable=False)
    categoria_produto = Column(String, nullable=False)
    valor_unitario = Column(Integer)

    def __repr__(self):
        return f"<dim_produto(nome='{self.nome_produto}')>"

class fato_registro_oportunidade(Base):
    __tablename__ = 'fato_registro_oportunidade'
    id_oportunidade = Column(String, primary_key=True, unique=True)
    id_parceiro = Column(String, ForeignKey('dim_parceiro.id_parceiro'))
    id_produto = Column(String, ForeignKey('dim_produto.id_produto'))
    data_registro = Column(String, nullable=False)
    quantidade = Column(Integer, nullable=False)
    valor_total = Column(Integer)
    status = Column(String, nullable=False)

    def __repr__(self):
        return f"<fato_registro_oportunidade(id='{self.id_oportunidade}')>"
    

class fato_sellout(Base):
    __tablename__ = 'fato_sellout'
    id_sellout = Column(String, primary_key=True, unique=True)
    id_parceiro = Column(String, ForeignKey('dim_parceiro.id_parceiro'))
    id_produto = Column(String, ForeignKey('dim_produto.id_produto'))
    data_fatura = Column(String, nullable=False)
    nf = Column(String, nullable=False)
    quantidade = Column(Integer, nullable=False)
    valor_total = Column(Integer)

    def __repr__(self):
        return f"<fato_sellout(id='{self.id_sellout}')>"

# Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)

SessionLocal = sessionmaker(autoflush=False, autocommit=False, bind=engine)


@contextmanager
def get_session():
    session = SessionLocal()

    try:
        yield session
    finally:
        session.close()

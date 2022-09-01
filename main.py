from pytterns.Builder import Builder
from .etlzator.load.LoadS3 import LoadS3
from .etlzator.etl.ETL import ETL
from .etlzator.extract.ExtractS3 import ExtractS3
from .etlzator.transform.Transform import Transform

contribuinte = (ExtractS3()
                .plataform('spark')
                .format('parquet')
                .repository('sefaz-ce-datalake-dev')
                .layer('raw')
                .schema('EXAPROD')
                .entity('CADASTRO/CONTRIBUINTE')
                .columns(["COD_CNPJ", "SEQ_CONTRIBUINTE"])
                .reference('df_contribuinte')
                .build)

print("Consumo de contribuinte concluido")


colunas_nota_fiscal_eletronica = ["SEQ_NOTA_FISCAL", "NUM_DOC_FISCAL", "NUM_SERIE", "DAT_EMISSAO_DOC_FISCAL", "COD_CNPJ_DESTINATARIO",
                                  "COD_CNPJ_EMITENTE", "NUM_ID", "SEQ_DATA_EMISSAO", "COD_RESULTADO_PROCESSAMEN", "TIP_DOC_FISCAL", "COD_MODELO_DOC_FISCAL"]
particoes_nota_fiscal_eletronica = ['2021', '01', '*']
nota_fiscal_eletronica = (ExtractS3()
                          .plataform('spark')
                          .format('parquet')
                          .repository('sefaz-ce-datalake-dev')
                          .layer('raw')
                          .schema('EXANFECORP')
                          .entity('NFECORP/NOTA_FISCAL_ELETRONICA')
                          .columns(colunas_nota_fiscal_eletronica)
                          .partition(particoes_nota_fiscal_eletronica)
                          .reference('df_nota_fiscal_eletronica')
                          .build)

print("Consumo nota fiscal eletronica")

@Builder
class ObterNotasFiscaisCE(Transform):
    def __init__(self) -> None:
        self.colunas_chaves = ["SEQ_NOTA_FISCAL",
                               "NUM_DOC_FISCAL",
                               "NUM_SERIE",
                               "DAT_EMISSAO_DOC_FISCAL",
                               "COD_CNPJ_DESTINATARIO",
                               "COD_CNPJ_EMITENTE",
                               "NUM_ID",
                               "SEQ_DATA_EMISSAO",
                               "COD_RESULTADO_PROCESSAMEN",
                               "TIP_DOC_FISCAL",
                               "COD_MODELO_DOC_FISCAL"
                               ]
        self.df_contribuinte = None
        self.df_nota_fiscal_eletronica = None

    def execute(self):
        print(self.df_contribuinte, self.df_nota_fiscal_eletronica)
        return 'df_nota_fiscal_eletronica_ce', 'teste-execute'


nota_fiscal_ce = (LoadS3()
                  .plataform('spark')
                  .format('parquet')
                  .repository('sefaz-ce-datalake-dev')
                  .layer('refined')
                  .schema('NOTAS_FISCAIS')
                  .entity('CONTRIBUINTES')
                  .reference('df_nota_fiscal_eletronica_ce')
                  .build)

(ETL()
 .extract(contribuinte)
 .extract(nota_fiscal_eletronica)
 .transform(ObterNotasFiscaisCE)
 .load(nota_fiscal_ce)
 )

print("ETL concluído com sucesso")

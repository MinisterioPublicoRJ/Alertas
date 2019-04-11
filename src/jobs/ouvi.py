from shared.base import spark
from shared.timer import Timer

def alerta_ouvi():
    columns = [
        'docu_dk', 
        'docu_nr_mp', 
        'docu_nr_externo', 
        'docu_tx_etiqueta', 
        'docu_dt_cadastro', 
        'cldc_ds_classe', 
        'cldc_ds_hierarquia', 
        'movi_orga_dk_destino'
    ]

    print('Calculando alertas de Expediente de Ovidoria pendente de recebimento')
    
    with Timer():
        documento = spark.table('exadata.mcpr_documento')
        classe = spark.table('exadata_aux.mmps_classe_hierarquia')
        item_mov = spark.table('exadata.mcpr_item_movimentacao')
        mov = spark.table('exadata.mcpr_movimentacao')
        doc_classe = documento.join(classe, documento.docu_cldc_dk == classe.CLDC_DK, 'left')
        doc_mov = item_mov.join(mov, item_mov.item_movi_dk == mov.movi_dk, 'inner')

        return doc_classe.join(doc_mov, doc_classe.docu_dk == doc_mov.item_docu_dk, 'inner').\
            filter('docu_tpdc_dk = 119').\
            filter('docu_tpst_dk != 11').\
            filter('item_in_recebimento IS NULL').\
            filter('movi_tpgu_dk == 2').\
            filter('movi_dt_recebimento_guia IS NULL').\
            select(columns)

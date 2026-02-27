import sys
import logging
from src import pipeline
from src import utils # Importando o utils para acessar o setup de logs

def main():
    # 1. ATIVA OS LOGS ANTES DE TUDO!
    utils.setup_logging()
    logger = logging.getLogger("MAIN")
    
    logger.info(">>> INICIANDO CÁLCULO DO ÍNDICE DE JUSTIÇA CLIMÁTICA <<<")
    
    try:
        # Chama a função orquestradora principal definida no src/pipeline.py
        pipeline.run()
        
        logger.info(">>> PROCESSO FINALIZADO COM SUCESSO! <<<")
        logger.info("Verifique a pasta 'data/results/' para os arquivos gerados.")
        
    except KeyboardInterrupt:
        logger.warning(" Processo interrompido pelo usuário (Ctrl+C).")
        sys.exit(0)
        
    except Exception as e:
        logger.critical(f"ERRO NÃO TRATADO: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
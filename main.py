import sys
import logging
from src import pipeline

# Configuração Global de Logs
# Isso garante que você veja no terminal o que está acontecendo dentro de utils, calculations e pipeline
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)

logger = logging.getLogger("MAIN")

def main():
    logger.info(">>> INICIANDO CÁLCULO DO ÍNDICE DE JUSTIÇA CLIMÁTICA <<<")
    
    try:
        # Chama a função orquestradora principal definida no src/pipeline.py
        # Ela é responsável por rodar tanto a rotina de setores quanto a de H3
        pipeline.run()
        
        logger.info(">>> PROCESSO FINALIZADO COM SUCESSO! <<<")
        logger.info("Verifique a pasta 'data/clean/' para os arquivos gerados.")
        
    except KeyboardInterrupt:
        logger.warning(" Processo interrompido pelo usuário (Ctrl+C).")
        sys.exit(0)
        
    except Exception as e:
        logger.critical(f"ERRO NÃO TRATADO: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
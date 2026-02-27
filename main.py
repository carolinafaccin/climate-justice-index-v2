import sys
import logging
from src import pipeline
from src import utils # Importing utils to access the logging setup

def main():
    # 1. ACTIVATE LOGS FIRST!
    utils.setup_logging()
    logger = logging.getLogger("MAIN")
    
    logger.info(">>> STARTING CLIMATE JUSTICE INDEX CALCULATION <<<")
    
    try:
        # Calls the main orchestrator function defined in src/pipeline.py
        pipeline.run()
        
        logger.info(">>> PROCESS COMPLETED SUCCESSFULLY! <<<")
        logger.info("Check the 'data/outputs/results/' folder for the generated files.")
        
    except KeyboardInterrupt:
        logger.warning(" Process interrupted by the user (Ctrl+C).")
        sys.exit(0)
        
    except Exception as e:
        logger.critical(f"UNHANDLED ERROR: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
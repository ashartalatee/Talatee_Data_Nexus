import os
import logging
import pandas as pd
from src.ingestion.loader import DataLoader
from src.cleaning.rename import RenameModule
from src.cleaning.missing import MissingHandler
from src.cleaning.duplicates import DuplicateHandler
from src.cleaning.formatter import FormatterModule
from src.validation.schema_validator import SchemaValidator

class PipelineRunner:
    """
    The heart of the engine. Orchestrates the flow of data from ingestion
    through various cleaning modules to the final output.
    """

    def __init__(self, config: dict, schema: dict, logger: logging.Logger):
        self.config = config
        self.schema = schema
        self.logger = logger
        
        # Initialize Core Modules
        self.loader = DataLoader(logger)
        self.validator = SchemaValidator(logger)
        self.renamer = RenameModule(logger)
        self.missing_handler = MissingHandler(logger)
        self.duplicate_handler = DuplicateHandler(logger)
        self.formatter = FormatterModule(logger)

    def run(self):
        """
        Executes the full pipeline: Load -> Validate -> Rename -> Clean -> Save.
        """
        client_name = self.config["client_metadata"]["client_name"]
        io_settings = self.config.get("io_settings", {})
        
        input_dir = f"data/{client_name}/input"
        output_dir = f"data/{client_name}/output"
        
        # Ensure output directory exists
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Get all files from input directory
        files = [f for f in os.listdir(input_dir) if not f.startswith('.')]
        
        if not files:
            self.logger.warning(f"No input files found in {input_dir}")
            return

        for file_name in files:
            try:
                self.logger.info(f"--- Processing File: {file_name} ---")
                file_path = os.path.join(input_dir, file_name)
                
                # 1. INGESTION
                # Pass encoding from config if exists
                load_params = {"encoding": io_settings.get("encoding", "utf-8")}
                df = self.loader.load_file(file_path, **load_params)

                # 2. RENAMING (Standardize headers first)
                df = self.renamer.process(df, self.schema)

                # 3. VALIDATION (Check against schema.json requirements)
                self.validator.validate(df, self.schema)

                # 4. CLEANING: DUPLICATES
                dup_rules = self.config["cleaning_rules"].get("drop_duplicates", {})
                df = self.duplicate_handler.process(df, dup_rules)

                # 5. CLEANING: MISSING VALUES
                missing_rules = self.config["cleaning_rules"].get("handle_missing", {})
                df = self.missing_handler.process(df, missing_rules)

                # 6. FORMATTING (Casing, Dates)
                format_rules = self.config["cleaning_rules"]
                df = self.formatter.process(df, format_rules)

                # 7. OUTPUT
                output_prefix = io_settings.get("output_prefix", "clean_")
                output_filename = f"{output_prefix}{os.path.splitext(file_name)[0]}.csv"
                output_path = os.path.join(output_dir, output_filename)
                
                df.to_csv(output_path, index=False, encoding=io_settings.get("encoding", "utf-8"))
                self.logger.info(f"Pipeline completed. Output saved to: {output_path}")

            except Exception as e:
                self.logger.error(f"Pipeline failed for file {file_name}: {str(e)}")
                # Continue to next file instead of crashing the whole process
                continue
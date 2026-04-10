import json
import os
import logging

class FileHandler:
    """
    Utility class for safe file operations including JSON loading
    and directory management.
    """

    @staticmethod
    def load_json(path: str) -> dict:
        """
        Loads and parses a JSON file. Raises error if file is missing or invalid.
        """
        if not os.path.exists(path):
            raise FileNotFoundError(f"Required configuration file missing: {path}")

        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON format in {path}: {str(e)}")
        except Exception as e:
            raise Exception(f"Unexpected error loading {path}: {str(e)}")

    @staticmethod
    def ensure_dir(directory_path: str):
        """
        Ensures a directory exists, creates it if not.
        """
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)

    @staticmethod
    def list_files(directory_path: str, extension: str = None) -> list:
        """
        Lists files in a directory, optionally filtered by extension.
        """
        if not os.path.exists(directory_path):
            return []
        
        files = [f for f in os.listdir(directory_path) if os.path.isfile(os.path.join(directory_path, f))]
        
        if extension:
            return [f for f in files if f.lower().endswith(extension.lower())]
        
        return files
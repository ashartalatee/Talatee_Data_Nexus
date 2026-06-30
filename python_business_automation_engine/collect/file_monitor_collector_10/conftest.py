import sys
from pathlib import Path

# Tambahkan root folder ke Python path agar 'engine' bisa ditemukan
sys.path.insert(0, str(Path(__file__).parent))
from pathlib import Path
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # Calculate the root of the project
    BASE_DIR: Path = Path(__file__).resolve().parent.parent
    
    # Define our standard folders
    DATA_DIR: Path = BASE_DIR / "data"
    OUTPUT_DIR: Path = BASE_DIR / "output"
    
    # Global constants for our experiments
    DEFAULT_ROWS: int = 1_000_000
    
    def model_post_init(self, __context):
        # Auto-create folders on startup
        self.DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Create the instance that other files will use
settings = Settings()
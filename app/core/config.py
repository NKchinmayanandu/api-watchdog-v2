from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    PROJECT_NAME: str = "API-WATCHDOG-V2"
    VERSION: str = "0.2.0"
    API_PREFIX: str = "/api"

    DATABASE_URL: str
    SECRET_KEY: str
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60

    REDIS_URL: str = "redis://localhost:6379/0"
    TELEGRAM_API_ID : int
    TELEGRAM_API_HASH : str
    TELEGRAM_SESSION_PATH : str
    TELEGRAM_USERNAME: str
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    @property
    def async_database_url(self) -> str:
        return self.DATABASE_URL.replace(
            "postgresql://", "postgresql+asyncpg://", 1
        )
    
settings = Settings()
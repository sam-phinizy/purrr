from pathlib import Path
import tomllib
from pydantic_settings import BaseSettings


class PurrrSettings(BaseSettings):
    """Settings for the Purrr application."""

    pre_fetch_logs: bool = True

    @classmethod
    def load(cls, config_path: Path | None = None) -> "PurrrSettings":
        """Load settings from config files and environment variables.

        Looks for config files in the following order:
        1. ~/.config/purrr/config.toml
        2. ./.purrr.toml (current directory)
        3. Environment variables (these take precedence)
        """
        config_data = {}

        # Check home directory
        home_config = Path.home() / ".config" / "purrr" / "config.toml"
        if home_config.exists():
            with open(home_config, "rb") as f:
                config_data.update(tomllib.load(f))
        # Check current directory
        current_config = Path(".purrr.toml")
        if current_config.exists():
            with open(current_config, "rb") as f:
                config_data.update(tomllib.load(f))

        # Create settings instance (env vars will automatically override)
        return cls(**config_data)


# Global settings instance
settings = PurrrSettings.load()

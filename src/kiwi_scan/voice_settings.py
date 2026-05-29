"""Voice scan settings and configuration.

Manages user settings for voice scanning:
- Speech-to-text engine selection
- Audio quality parameters
- Memory channel persistence
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, asdict, field
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Config file location
VOICE_CONFIG_FILE = Path.home() / ".kiwiscan" / "voice_config.json"


@dataclass
class VoiceScanSettings:
    """User settings for voice scanning feature."""
    
    # STT engine selection
    stt_engine: str = "none"  # "none", "whisper", "google_cloud", "vosk"
    stt_enabled: bool = False
    
    # Whisper settings (local)
    whisper_model: str = "base"  # "tiny", "base", "small", "medium", "large"
    whisper_device: str = "cpu"  # "cpu", "cuda", "mps"
    
    # Google Cloud settings
    google_cloud_key_path: Optional[str] = None
    
    # Audio capture settings
    capture_enabled: bool = True
    capture_directory: str = "/tmp/kiwiscan_voice"
    capture_max_duration_s: float = 10.0
    
    # Scanning behavior
    auto_fine_tune: bool = True  # Auto-tune to carrier offset when detected
    min_confidence_threshold: float = 0.5  # 0.0–1.0
    
    # Memory channel behavior
    memory_persist_session_only: bool = True  # False = persist across sessions
    auto_clear_on_mode_change: bool = True
    
    def __post_init__(self):
        """Validate settings."""
        if self.stt_engine not in ("none", "whisper", "google_cloud", "vosk"):
            self.stt_engine = "none"
        if self.whisper_model not in ("tiny", "base", "small", "medium", "large"):
            self.whisper_model = "base"
        if self.whisper_device not in ("cpu", "cuda", "mps"):
            self.whisper_device = "cpu"
        self.min_confidence_threshold = max(0.0, min(1.0, self.min_confidence_threshold))
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: dict) -> VoiceScanSettings:
        """Create from dictionary (JSON deserialization)."""
        # Filter to only known fields
        known_fields = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in data.items() if k in known_fields}
        return cls(**filtered)


class VoiceSettingsManager:
    """Manages voice scan settings persistence."""
    
    def __init__(self, config_file: Path = VOICE_CONFIG_FILE):
        self.config_file = Path(config_file)
        self._settings = VoiceScanSettings()
        self._load()
    
    def get_settings(self) -> VoiceScanSettings:
        """Get current settings."""
        return self._settings
    
    def update_settings(self, **kwargs) -> None:
        """Update settings fields and persist.
        
        Args:
            **kwargs: Field names and values to update
        """
        for key, value in kwargs.items():
            if hasattr(self._settings, key):
                setattr(self._settings, key, value)
            else:
                logger.warning(f"Unknown setting: {key}")
        
        # Re-validate after update
        self._settings.__post_init__()
        self._save()
    
    def _load(self) -> None:
        """Load settings from file if it exists."""
        if not self.config_file.exists():
            logger.info(f"No voice config found at {self.config_file}, using defaults")
            return
        
        try:
            with open(self.config_file, "r") as f:
                data = json.load(f)
            self._settings = VoiceScanSettings.from_dict(data)
            logger.info(f"Loaded voice settings from {self.config_file}")
        except Exception as err:
            logger.error(f"Failed to load voice settings: {err}. Using defaults.")
            self._settings = VoiceScanSettings()
    
    def _save(self) -> None:
        """Save settings to file."""
        try:
            self.config_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.config_file, "w") as f:
                json.dump(self._settings.to_dict(), f, indent=2)
            logger.info(f"Saved voice settings to {self.config_file}")
        except Exception as err:
            logger.error(f"Failed to save voice settings: {err}")
    
    def to_json_dict(self) -> dict:
        """Export settings for API response (JSON-safe)."""
        return self._settings.to_dict()


# Global settings manager instance
_settings_manager: Optional[VoiceSettingsManager] = None


def get_voice_settings_manager() -> VoiceSettingsManager:
    """Get or create global settings manager."""
    global _settings_manager
    if _settings_manager is None:
        _settings_manager = VoiceSettingsManager()
    return _settings_manager


def get_voice_settings() -> VoiceScanSettings:
    """Convenience function to get current settings."""
    return get_voice_settings_manager().get_settings()

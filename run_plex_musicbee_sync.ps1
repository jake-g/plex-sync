# Set Window Title
$Host.UI.RawUI.WindowTitle = "plex-musicbee-sync"

# --- Unified Music Sources Scripts ---
Set-Location $PSScriptRoot

# Plex
python plex_ratings_update_from_musicbee.py | Tee-Object -FilePath "plex_ratings_update_from_musicbee.log"

pause # (pause until enter pressed)

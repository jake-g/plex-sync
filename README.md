# Plex Sync

Sync ratings and playlists from MusicBee to Plex.

## Overview

This module provides scripts to synchronize track ratings and playlists from a local MusicBee library to a Plex media server. It ensures that your ratings and playlists are consistent across both platforms.

## Scripts

### 1. `plex_ratings_update_from_musicbee.py`

Syncs ratings and playlists from MusicBee to Plex.

- **Features**:
  - Updates track ratings in Plex based on MusicBee data.
  - Protects recently rated tracks in Plex from being overwritten (`RECENTLY_RATED_LIMIT`).
  - Syncs playlists from MusicBee to Plex.
  - Option to delete small playlists (`MIN_PLAYLIST_SIZE`).
  - **Dry Run Mode**: See what changes would be made without applying them.
- **Usage**:

    ```bash
    python plex_ratings_update_from_musicbee.py
    ```

- **Configuration**:
  - Edit variables in the script to toggle `DRY_RUN`, `SYNC_PLAYLISTS`, etc.

## Shared Library

- `plex_lib.py`: Library for interacting with Plex API (PlexMusic class).
- `unify_lib.py`: Shared library for path normalization.

## Authentication

Ensure `auth.py` contains your Plex credentials:

```python
PLEX_TOKEN = 'your_token'
PLEX_SERVER_URL = 'http://your_server:port'
```

## Setup

1. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

## Tests

To run the unit tests:

```bash
python plex_lib_tests.py
```

## Highlight Changelog

- **May 2026**: Modularized Plex directory with independent tests and CI workflows
- **Mar 2026**: Added functionality to clean up small playlists
- **Nov 2025**: Finalized 2-way rating and playlist synchronization between Plex and MusicBee
- **Aug 2024**: Transitioned from notebooks to dedicated Python scripts for Plex synchronization
- **Jun 2024**: Initial implementation of Plex Music API library and MusicBee rating sync

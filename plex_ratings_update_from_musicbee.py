"""Syncs ratings and playlists between MusicBee and Plex Music library.

Handles two-way rating synchronization:
- Overwrites Plex ratings with MusicBee ratings (MusicBee is master).
- Protects recently rated tracks on Plex and exports them to M3U playlists
  for MusicBee import.
- Synchronizes MusicBee M3U playlists to Plex.
"""

import os
import re
import sys
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set

import pandas as pd

# Configure sys.path so modules can be imported regardless of working directory
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
  sys.path.insert(0, SCRIPT_DIR)

UNIFIED_DIR = os.path.abspath(
    os.path.join(SCRIPT_DIR, '..', 'music-sources-unified')
)
if UNIFIED_DIR not in sys.path:
  sys.path.insert(0, UNIFIED_DIR)

# isort: split
from auth import PLEX_SERVER_URL, PLEX_TOKEN
from local_paths import (MB_PATHS_TO_IGNORE, MULTIPLE_OK,
                         MUSICBEE_PLAYLISTS_DIR, NUC_PC_PREFIX_MAP,
                         PLEX_FIX_PATHS)
from plex_lib import PlexMusic

import unify_lib as uni

# =============================================================================
# CONFIGURATION & CONSTANTS
# =============================================================================
# Force UTF-8 for unicode track titles and line buffering so progress streams
# live when piped (e.g. `python ... | Tee-Object`) instead of stalling in a
# block buffer until the run finishes.
if hasattr(sys.stdout, 'reconfigure'):
  sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)

# Plex Library Configuration
PLEX_LIBRARY_NAME = 'Music'  # Name of target Plex music library section
PLEX_SYNC_LOG_EVERY = 5000  # Progress log frequency (every N tracks)
PLEX_SLEEP_TIME_S = 0.01  # Micro-pause between API rate calls for LAN server
RECENTLY_RATED_LIMIT = 100  # Protect recently rated Plex tracks
MAX_PLAYLIST_SIZE = 20000  # Max playlist track count limit before skipping

# Playlist Cleanup Configuration
MIN_PLAYLIST_SIZE = 15  # Smaller playlists cleaned up

# Execution Modes
VERBOSE = True  # Output detailed per-track log messages
DRY_RUN = False  # False = Live mode (writes to Plex); True = Preview only
SYNC_PLAYLISTS = True  # True = Sync playlists to Plex; False = Ratings only

# UI & Display Options
SHOW_PLEX_INFO = True  # Display library overview statistics on startup
SHOW_RECENT_RATED = True  # Display recently rated tracks list on startup
SHOW_RECENT_ADDED = True  # Display recently added albums list on startup
N_RECENT_DISPLAY = 20  # Number of recent entries to show in startup summaries

# Directory Paths
OUTPUT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..', 'music-sources-unified')
)
ASSETS_DIR = os.path.join(OUTPUT_DIR, 'db_assets')
RATING_EXPORT_DIR = os.path.join(OUTPUT_DIR, 'm3u')

# MusicBee Databases
MUSICBEE_LIBRARY_TSV = os.path.join(ASSETS_DIR, 'musicbee_library.tsv')
MUSICBEE_INBOX_TSV = os.path.join(ASSETS_DIR, 'musicbee_inbox.tsv')


def load_plex_fix_keys() -> Set[str]:
  """Loads normalized path keys from hardcoded PLEX_FIX_PATHS."""
  return {uni.sanitize_path_for_matching(p) for p in PLEX_FIX_PATHS}


# =============================================================================
# HELPERS
# =============================================================================

def convert_nuc_to_pc_path(nuc_path: str) -> str:
  """Converts linux/nuc paths to windows/pc paths using global mapping.

  Normalizes slash conventions across platforms so prefix mapping succeeds
  consistently regardless of OS and forward/backward slash formats.

  Args:
    nuc_path: Raw filepath string from Plex metadata (e.g. /mnt/music/...).

  Returns:
    Mapped Windows PC filepath (e.g. B:\\Music\\...).
  """
  n_clean = nuc_path.replace('\\', '/')
  n_lower = n_clean.lower()
  for nuc_prefix, pc_prefix in NUC_PC_PREFIX_MAP.items():
    p_clean = nuc_prefix.replace('\\', '/')
    p_lower = p_clean.lower()
    if n_lower.startswith(p_lower):
      rel = n_clean[len(p_clean):].lstrip('/')
      pc_base = pc_prefix.rstrip('\\/')
      return f"{pc_base}\\{rel.replace('/', chr(92))}"
  return nuc_path


def create_plex_track_map(
    plex_tracks: List[Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
  """Creates a dictionary mapping normalized file paths to Plex track data.

  Key: Sanitized PC Path -> Value: {ratingKey, data, pc_path}

  Args:
    plex_tracks: List of raw Plex track dictionaries from API.

  Returns:
    Dictionary mapping normalized path keys to track info.
  """
  print(f'Mapping {len(plex_tracks)} Plex entries...', end=' ', flush=True)
  t_start = time.time()
  track_map = {}

  for track in plex_tracks:
    for media in track.get('Media', []):
      for part in media.get('Part', []):
        raw_filepath = part.get('file', '')
        if not raw_filepath:
          continue
        pc_filepath = convert_nuc_to_pc_path(raw_filepath)
        norm_key = uni.sanitize_path_for_matching(pc_filepath)
        track_map[norm_key] = {
            'pc_path': pc_filepath,
            'data': track,
            'ratingKey': track.get('ratingKey'),
        }

  print(
      f"Done in {time.time() - t_start:.2f}s. (Map Size: {len(track_map)})"
  )
  return track_map


def is_absolute_pc_path(path_str: str) -> bool:
  """Checks if a filepath is an absolute path on Windows or POSIX.

  Recognizes Windows drive letters (e.g. B:\\, C:/), UNC shares
  (\\\\server\\share), and POSIX root slashes (/).

  Args:
    path_str: Filepath string.

  Returns:
    True if the path is absolute, False if relative.
  """
  if not path_str:
    return False
  return (
      os.path.isabs(path_str)
      or bool(re.match(r'^[a-zA-Z]:[\\/]', path_str))
      or path_str.startswith('\\\\')
      or path_str.startswith('/')
  )


MULTIPLE_OK_SLUGS = tuple(
    uni.slugify(ok, strip_non_alphanum=True) for ok in MULTIPLE_OK
)


def is_whitelisted(path: str) -> bool:
  """Checks if a path is in the allowed duplicate whitelist."""
  clean_path = uni.slugify(path, strip_non_alphanum=True)
  return clean_path.startswith(MULTIPLE_OK_SLUGS)


def read_musicbee_playlists(
    input_dir: str,
    plex_map: Dict[str, Dict[str, Any]],
    counters: Dict[str, int],
    verbose: bool = True,
    min_size: int = MIN_PLAYLIST_SIZE,
) -> Dict[str, List[str]]:
  """Scans directory for M3U files, resolves paths against plex_map.

  Ignores paths found in MB_PATHS_TO_IGNORE. Breaks immediately if the
  playlist directory does not exist.

  Args:
    input_dir: Directory containing exported MusicBee M3U/M3U8 files.
    plex_map: Mapping of sanitized file paths to Plex track info.
    counters: Tracking dictionary for sync statistics.
    verbose: Whether to print detailed playlist processing logs.
    min_size: Minimum valid tracks required to include a playlist.

  Returns:
    Dictionary mapping playlist title to list of Plex ratingKey IDs.

  Raises:
    FileNotFoundError: If input_dir is empty or does not exist.
  """
  uni.print_section('READING MUSICBEE PLAYLISTS')
  t_start = time.time()

  if not input_dir or not os.path.exists(input_dir):
    raise FileNotFoundError(
        f"Playlist directory not found or not configured: '{input_dir}'. "
        f"Please verify MUSICBEE_PLAYLISTS_DIR in local_paths.py."
    )

  sync_plex_playlists = {}
  m3u_files = [
      os.path.join(r, f)
      for r, _, files in os.walk(input_dir)
      for f in files
      if f.lower().endswith(('.m3u', '.m3u8'))
  ]

  print(f"Found {len(m3u_files)} playlists. Resolving paths...", flush=True)
  ignore_prefixes = tuple(p.lower() for p in MB_PATHS_TO_IGNORE)

  for filepath in m3u_files:
    playlist_name = os.path.splitext(os.path.basename(filepath))[0]
    raw_paths = uni.load_m3u(filepath)

    valid_keys = []
    missing_count = 0
    ignored_count = 0
    total_tracks = len(raw_paths)

    for raw_path in raw_paths:
      raw_path = str(raw_path).strip()

      # 1. Resolve to Absolute PC Path
      if not is_absolute_pc_path(raw_path):
        full_pc_path = os.path.normpath(
            os.path.join(os.path.dirname(filepath), raw_path)
        )
      else:
        full_pc_path = os.path.normpath(raw_path)

      # 2. Check Ignore List
      if ignore_prefixes and full_pc_path.lower().startswith(ignore_prefixes):
        ignored_count += 1
        continue

      # 3. Lookup in Plex Map
      norm_key = uni.sanitize_path_for_matching(full_pc_path)
      if norm_key in plex_map:
        valid_keys.append(str(plex_map[norm_key]['ratingKey']))
      else:
        missing_count += 1

    # Record Stats
    counters['Playlist: Processed'] += 1
    counters['Playlist Tracks: Found'] += len(valid_keys)
    counters['Playlist Tracks: Missing'] += missing_count
    counters['Playlist Tracks: Ignored'] += ignored_count

    if len(valid_keys) > MAX_PLAYLIST_SIZE:
      counters['Playlist: Skipped (Too Large)'] += 1
      if verbose:
        print(
            f"    [Skip] '{playlist_name}' too large: {len(valid_keys)} tracks "
            f"(Limit: {MAX_PLAYLIST_SIZE})"
        )
    elif len(valid_keys) < min_size:
      if valid_keys:
        counters['Playlist: Skipped (Too Small)'] += 1
      else:
        counters['Playlist: Skipped (Empty/All Ignored)'] += 1
    else:
      sync_plex_playlists[playlist_name] = valid_keys

    if verbose:
      info = f"'{playlist_name}' -> {len(valid_keys)} found"
      if missing_count > 0:
        info += f", {missing_count} missing"
      if ignored_count > 0:
        info += f", {ignored_count} ignored"
      print(info)

      effective_total = total_tracks - ignored_count
      if effective_total > 5 and (missing_count / effective_total) > 0.2:
        print(f"    [Warn] High missing track count for '{playlist_name}'")

  print(f"Playlist processing finished in {time.time() - t_start:.2f}s.")
  return sync_plex_playlists


def build_mb_path_lookup(mb_df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
  """Builds an O(1) lookup mapping normalized path to first row and count.

  Replaces per-track `mb_grouped.get_group(plex_key)` and `.iloc[0]` calls
  across 95,000+ Plex tracks with a single linear pass over `mb_df`.

  Args:
    mb_df: Standardized MusicBee DataFrame with 'Path' and 'Rating' columns.

  Returns:
    Dictionary mapping normalized path key to {'Path', 'Rating', 'count'}.
  """
  lookup: Dict[str, Dict[str, Any]] = {}
  paths = mb_df['Path'].tolist()
  ratings = mb_df['Rating'].tolist()
  norm_paths = (
      mb_df['Path_Norm'].tolist()
      if 'Path_Norm' in mb_df.columns
      else [uni.sanitize_path_for_matching(p) for p in paths]
  )
  for norm_key, path, rating in zip(norm_paths, paths, ratings):
    existing = lookup.get(norm_key)
    if existing is None:
      lookup[norm_key] = {'Path': path, 'Rating': rating, 'count': 1}
    else:
      existing['count'] += 1
  return lookup


# =============================================================================
# MAIN EXECUTION
# =============================================================================
def main():
  """Main execution flow for syncing ratings and playlists."""
  t_total = time.time()

  # 1. Initialize Plex
  uni.print_section('INITIALIZING PLEX')
  t_init = time.time()
  plex_music = PlexMusic(
      PLEX_SERVER_URL, PLEX_TOKEN, library_name=PLEX_LIBRARY_NAME
  )
  if SHOW_PLEX_INFO:
    plex_music.display_libraries()
  print(f"Plex connected in {time.time() - t_init:.2f}s.")

  # 2. Get Recent Tracks
  print(f"\nFetching Top {RECENTLY_RATED_LIMIT} Recently Rated tracks...")
  t_recent = time.time()
  recent_tracks = plex_music.get_recently_rated_tracks(
      limit=RECENTLY_RATED_LIMIT
  )
  recent_rating_keys = {str(t['ratingKey']) for t in recent_tracks}
  print(
      f"Identified {len(recent_rating_keys)} recent tracks in "
      f"{time.time() - t_recent:.2f}s."
  )

  if SHOW_RECENT_RATED:
    print('\n[Plex] Recently Rated:')
    plex_music.display_tracks(
        recent_tracks[:N_RECENT_DISPLAY], show_details=True
    )

  if SHOW_RECENT_ADDED:
    print('\n[Plex] Recently Added:')
    try:
      pl_tracks = plex_music.get_playlist_tracks('Recently Added')
      plex_music.display_tracks(
          pl_tracks[:N_RECENT_DISPLAY], show_details=True
      )
    except Exception:
      pass

  # 3. Create Plex Map
  uni.print_section('BUILDING PLEX MAP')
  t_map = time.time()
  plex_tracks = plex_music.get_tracks()
  print(f'Fetched {len(plex_tracks)} tracks metadata.')
  plex_map = create_plex_track_map(plex_tracks)

  # 4. Pre-Process MusicBee Playlists
  counters = defaultdict(int)
  sync_plex_playlists = {}
  if SYNC_PLAYLISTS:
    sync_plex_playlists = read_musicbee_playlists(
        MUSICBEE_PLAYLISTS_DIR, plex_map, counters, verbose=VERBOSE
    )

  # 5. Load MusicBee Data
  uni.print_section('LOADING MUSICBEE DB')
  t_mb = time.time()
  mb_df = uni.ingest_musicbee_db_assets(
      MUSICBEE_LIBRARY_TSV, MUSICBEE_INBOX_TSV, save_tsv=False
  )
  mb_df['Path_Norm'] = mb_df['Path'].apply(uni.sanitize_path_for_matching)
  mb_df = uni.mb_standardize_ratings(mb_df)
  mb_lookup = build_mb_path_lookup(mb_df)
  print(f'Loaded {len(mb_df)} tracks in {time.time() - t_mb:.2f}s.')

  # Load Hardcoded Plex Fix Overrides
  plex_fix_keys = load_plex_fix_keys()
  if plex_fix_keys:
    print(
        f'Loaded {len(plex_fix_keys)} hardcoded ignore keys from '
        f'PLEX_FIX_PATHS.'
    )

  # 6. Main Sync Loop (Ratings & Likes)
  export_musicbee_playlists = []
  uni.print_section('SYNCING RATINGS')
  print(f"Starting Sync... (DRY RUN: {DRY_RUN})")
  t_sync = time.time()

  for i, (plex_key, plex_val) in enumerate(plex_map.items()):
    if i % PLEX_SYNC_LOG_EVERY == 0:
      print(f'{i / len(plex_map):0.0%} processed...', flush=True)
    plex_data, plex_path = plex_val['data'], plex_val['pc_path']
    track_title = plex_data.get('title', '')
    rating_key = str(plex_val['ratingKey'])

    # Metadata Fixes
    if not track_title or track_title.strip().lower() == 'blank':
      counters['Metadata: Fixed from Filename'] += 1

    # Match Logic
    mb_track_data = mb_lookup.get(plex_key)
    if mb_track_data is None:
      counters['No Match (Plex Track not in MB)'] += 1
      continue

    # Duplicate Detection
    if mb_track_data['count'] > 1:
      if is_whitelisted(plex_path):
        counters['Match (Duplicate Allowed)'] += 1
      else:
        counters['Match (Duplicate Warning)'] += 1
        if VERBOSE:
          print(f"WARN: Multiple matches for {plex_path}")
    else:
      counters['Match Found'] += 1

    # Rating Sync
    raw_user_rating = plex_data.get('userRating')
    p_rating = (
        float(raw_user_rating) / 2.0 if raw_user_rating is not None else 0.0
    )
    m_rating = (
        float(mb_track_data['Rating'])
        if pd.notna(mb_track_data['Rating'])
        else 0.0
    )

    # Cleanup Invalid Plex Ratings
    if p_rating > 0 and (p_rating % 0.5 != 0):
      if VERBOSE:
        print(
            f"  CLEANUP: {track_title} (Plex: {p_rating}) -> Unrated "
            f"[Invalid Rating]"
        )
      if not DRY_RUN:
        plex_music.get_track(rating_key).rate(0)
        time.sleep(PLEX_SLEEP_TIME_S)
      counters['Plex Force Clean (Invalid)'] += 1
      p_rating = 0.0

    # Conflict Resolution
    if abs(p_rating - m_rating) > 0.1:
      if (
          rating_key in recent_rating_keys
          and p_rating > 0
          and plex_key not in plex_fix_keys
      ):
        # Plex is Newer -> Export M3U
        export_musicbee_playlists.append(
            {'Path': mb_track_data['Path'], 'Rating': p_rating}
        )
        counters['Conflict: Recent Plex Rating (Export M3U)'] += 1
        if VERBOSE:
          print(
              f"  [Mismatch]: Add {track_title} to rating .m3u "
              f"(Plex: {p_rating} | MB: {m_rating})"
          )
      else:
        # MB is Master -> Overwrite Plex
        tag = (
            'MB Master / Plex Fix'
            if plex_key in plex_fix_keys
            else 'MB Trumps'
        )
        if not DRY_RUN:
          plex_music.get_track(rating_key).rate(int(m_rating * 2))
          counters[f'Plex Updated ({tag})'] += 1
          time.sleep(PLEX_SLEEP_TIME_S)
        else:
          counters[f'Dry Run: MB Overwrite ({tag})'] += 1
          if VERBOSE:
            print(
                f"  [DRY] MB Overwrite ({tag}): {track_title} "
                f"(Plex: {p_rating} -> {m_rating})"
            )
    else:
      counters['Ratings Match'] += 1
      if p_rating > 0:
        counters['Total Rated Matches'] += 1
      else:
        counters['Matched (Unrated)'] += 1

  print(f"Rating sync finished in {time.time() - t_sync:.2f}s.")

  # 7. Commit Playlists
  if SYNC_PLAYLISTS and sync_plex_playlists:
    uni.print_section('SYNCING PLEX PLAYLISTS')
    print(f"Syncing {len(sync_plex_playlists)} playlists...", flush=True)
    t_pl_sync = time.time()

    # Fetch once for speed
    existing_playlists_map = {p.title: p for p in plex_music.get_playlists()}
    for name, keys in sync_plex_playlists.items():
      if len(keys) > MAX_PLAYLIST_SIZE:
        counters['Playlist: Skipped (Too Large)'] += 1
        if VERBOSE:
          print(
              f"  [Skip] '{name}' ({len(keys)} tracks > {MAX_PLAYLIST_SIZE})",
              flush=True,
          )
        continue

      existing_pl = existing_playlists_map.get(name)
      if existing_pl and getattr(existing_pl, 'leafCount', None) == len(keys):
        counters['Playlist: Skipped (Track Count Match)'] += 1
        continue

      if not DRY_RUN:
        t_single = time.time()
        if plex_music.sync_playlist(name, keys, existing_playlist=existing_pl):
          counters['Playlist: Created/Updated'] += 1
          if VERBOSE:
            print(
                f"  [Sync] '{name}' ({len(keys)} tracks) in "
                f"{time.time() - t_single:.1f}s",
                flush=True,
            )
        else:
          counters['Playlist: Failed'] += 1
          if VERBOSE:
            print(
                f"  [Fail] '{name}' failed after "
                f"{time.time() - t_single:.1f}s",
                flush=True,
            )
      else:
        counters['Playlist: Dry Run Skip'] += 1
    print(f"Playlist sync finished in {time.time() - t_pl_sync:.2f}s.")

    # Playlist Cleanup
    uni.print_section('CLEANING UP SMALL PLAYLISTS')
    print(
        'Checking for playlists with fewer than',
        f'{MIN_PLAYLIST_SIZE} tracks...',
    )
    num_deleted = plex_music.cleanup_small_playlists(
        min_size=MIN_PLAYLIST_SIZE, dry_run=DRY_RUN
    )
    counters['Playlist: Deleted (Too Small)'] = num_deleted

  # 8. Final Report
  uni.print_section('SYNC SUMMARY')
  for k, v in sorted(counters.items()):
    print(f"{k:<35}: {v}")

  # 9. Export Ratings
  if export_musicbee_playlists:
    print(f"\nWriting M3U Exports to '{RATING_EXPORT_DIR}'...")
    if not os.path.exists(RATING_EXPORT_DIR):
      os.makedirs(RATING_EXPORT_DIR)
    df = pd.DataFrame(export_musicbee_playlists)
    for rating, group in df.groupby('Rating'):
      fname = f"plex_ratings_{rating}.m3u"
      uni.save_m3u(
          os.path.join(RATING_EXPORT_DIR, fname), group['Path'].tolist()
      )
      print(f"  -> {fname}: {len(group)} tracks")

  print(f'\nDone in {(time.time() - t_total) / 60:0.1f}m.')


if __name__ == '__main__':
  import argparse
  parser = argparse.ArgumentParser(
      description='Sync ratings between MusicBee and Plex.'
  )
  parser.add_argument(
      '--dry-run',
      action='store_true',
      help='Preview changes without writing to Plex.',
  )
  parser.add_argument(
      '--no-playlists',
      action='store_true',
      help='Skip playlist synchronization.',
  )
  args = parser.parse_args()
  if args.dry_run:
    DRY_RUN = True
  if args.no_playlists:
    SYNC_PLAYLISTS = False
  main()

from collections import defaultdict
import os
import sys
import time

import pandas as pd

# Local import
module_path = os.path.abspath(os.path.join('..', 'music-sources-unified'))
if module_path not in sys.path:
    sys.path.append(module_path)
import unify_lib as uni

from auth import PLEX_SERVER_URL
from auth import PLEX_TOKEN
from plex_lib import PlexMusic

# =============================================================================
# CONFIGURATION & CONSTANTS
# =============================================================================
sys.stdout.reconfigure(encoding='utf-8')

# Plex Settings
PLEX_LIBRARY_NAME = 'Music'
PLEX_SYNC_LOG_EVERY = 5000
PLEX_SLEEP_TIME_S = 0.05
RECENTLY_RATED_LIMIT = 100      # Protect these from being overwritten by old MB data
MAX_PLEX_PLAYLIST_SIZE = 100000  # Skip playlists larger than this

# Playlist Settings
MIN_PLAYLIST_SIZE = 15  # Delete playlists with fewer than this many tracks

# Execution Modes
VERBOSE = True
DRY_RUN = False  # Live Mode: Changes will be written to Plex
SYNC_PLAYLISTS = True

# UI Options
SHOW_PLEX_INFO = True
SHOW_RECENT_RATED = True
SHOW_RECENT_ADDED = True
N_RECENT_DISPLAY = 20

# Directory Paths
# Root directory for unified music source outputs
OUTPUT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'music-sources-unified'))
ASSETS_DIR = os.path.join(OUTPUT_DIR, 'db_assets')

# MusicBee Databases
MUSICBEE_LIBRARY_TSV = os.path.join(ASSETS_DIR, 'musicbee_library.tsv')
MUSICBEE_INBOX_TSV = os.path.join(ASSETS_DIR, 'musicbee_inbox.tsv')

# Input: Playlists exported from MusicBee
MUSICBEE_PLAYLISTS_DIR = 'D:\\Music\\MusicBee\\mb_playlists'

# Output: Ratings exported from Plex
RATING_EXPORT_DIR = os.path.join(OUTPUT_DIR, 'm3u')

# Ignore List: Paths in MusicBee/M3Us that are deliberately NOT in Plex.
MB_PATHS_TO_IGNORE = [
    'D:\\Music\\Unsorted',
]

# Mapping: Converts Linux/NUC paths (Plex) to Windows/PC paths (MusicBee)
NUC_PC_PREFIX_MAP = {
    '\\data\\seed_archive\\': 'B:\\seed_archive\\',
    '\\mnt\\music\\': 'D:\\Music\\',
}

# Whitelist: Folders where duplicate albums/tracks are allowed
MULTIPLE_OK = [
    "b:\\seed_archive\\music_library\\1000 Gecs",
    "b:\\seed_archive\\music\\∩╝Æ∩╝ÿ∩╝æ∩╝ö - µû░πüùπüäµùÑπü«Φ¬òτöƒ",
    "b:\\seed_archive\\music_library\\Altin G├╝n - A┼ƒk",
    "b:\\seed_archive\\music_library\\Andre╠ü 3000 - New Blue Sun",
    "d:\\Music\\Library\\Aphex Twin\\Selected Ambient Works, Volume II",
    "b:\\seed_archive\\music\\Azmari - Sam─ü'─½",
    "b:\\seed_archive\\music\\A Winged Victory for the Sullen",
    "b:\\seed_archive\\music\\Babyshambles - Down In Albion",
    "b:\\seed_archive\\music_library\\BADBADNOTGOOD - Late Night Tales",
    "b:\\seed_archive\\music\\LateNightTales - Belle & Sebastian Vol. 2",
    "b:\\seed_archive\\music_library\\Bj├╢rk - Debut",
    "b:\\seed_archive\\music_library\\Bj├╢rk - fossora",
    "b:\\seed_archive\\music_library\\Bj├╢rk -2015- Vulnicura",
    "b:\\seed_archive\\music_library\\Boris - Smile",
    "b:\\seed_archive\\music_library\\Burzum - Filosofem",
    "b:\\seed_archive\\music\\Dauwd - 2013 - Heat Division Remixes",
    "b:\\seed_archive\\music_library\\David Bowie - Heroes",
    "b:\\seed_archive\\music_library\\Django Reinhardt - Djangology",
    "b:\\seed_archive\\music_library\\Fleet Foxes - Crack-Up",
    "b:\\seed_archive\\music\\Fran├ºoise Hardy - Comment te dire adieu",
    "b:\\seed_archive\\music_library\\Glass Animals - Remixes",
    "b:\\seed_archive\\music\\Helado Negro - Far In",
    "b:\\seed_archive\\music_library\\Hot Sugar - Seductive Nightmares 2",
    "b:\\seed_archive\\music_library\\Janelle Mon├íe - Dirty Computer",
    "b:\\seed_archive\\music_library\\Jean Michel Jarre - 1976 - Oxygene",
    "b:\\seed_archive\\music_library\\Keith Jarrett - The Ko╠êln Concert",
    "b:\\seed_archive\\music\\Kruder & Dorfmeister - The K&D Sessions",
    "b:\\seed_archive\\music\\L'Impe╠üratrice - Tako Tsubo",
    "b:\\seed_archive\\music\\Laryssa Kim - Contezza",
    "b:\\seed_archive\\music_library\\The Lions - Soul Riot",
    "b:\\seed_archive\\music\\Los Lobos - How Will the Wolf Survive",
    "b:\\seed_archive\\music_lossy\\Luc Ferrari - Presque Rien",
    "b:\\seed_archive\\music_library\\Lusine - Sensorimotor",
    "b:\\seed_archive\\music\\Lykke Li - So Sad So Sexy",
    "b:\\seed_archive\\music\\Mach-Hommy - Pray For Haiti",
    "b:\\seed_archive\\music_library\\Madlib - Sound Ancestors",
    "b:\\seed_archive\\music\\Magma - Me╠êkani╠êk De╠êstrukti╠êw╠Ç Ko╠êmmando╠êh",
    "b:\\seed_archive\\music_library\\Nicolas Jaar - Cenizas",
    "b:\\seed_archive\\music_library\\Nicolas Jaar - Pomegranates",
    "b:\\seed_archive\\music_library\\Nosaj Thing - Continua",
    "b:\\seed_archive\\music\\ßäæßàíßäàßàíßå½ (Parannoul)",
    "b:\\seed_archive\\music_library\\Plat - Compulsion",
    "b:\\seed_archive\\music\\Potatoi - Toy",
    "b:\\seed_archive\\music\\Rammstein - Zeit",
    "b:\\seed_archive\\music_library\\Ricky Eat Acid - You get sick",
    "b:\\seed_archive\\music_library\\R├╢yksopp - Profound Mysteries",
    "b:\\seed_archive\\music_library\\Santana - Supernatural",
    "b:\\seed_archive\\music\\ScHoolboy Q - Setbacks",
    "b:\\seed_archive\\music_library\\Skinshape - Umoja",
    "b:\\seed_archive\\music\\Sofia Kourtesis - Madres",
    "b:\\seed_archive\\music_library\\Sonic Youth - 1992 - Dirty",
    "b:\\seed_archive\\music_library\\Stan Getz - 1976 - The Best of Two Worlds",
    "b:\\seed_archive\\music_library\\Stan Getz & Jo├úo Gilberto - Getz-Gilberto",
    "b:\\seed_archive\\music\\Sunn O))) - Monoliths & Dimensions",
    "b:\\seed_archive\\music\\Takeuchi, Mariya - Variety",
    "b:\\seed_archive\\music_library\\Thriftworks - Rainmaker",
    "b:\\seed_archive\\music_library\\Todd Terje - It's Album Time",
    "b:\\seed_archive\\music_library\\Vieux Farka Tour├⌐ & Khruangbin - Ali",
    "b:\\seed_archive\\music_library\\Young Montana - Limerence",
    "b:\\seed_archive\\music_lossy\\Yumi Zouma (2014) - Yumi Zouma",
]


# =============================================================================
# HELPERS
# =============================================================================

def convert_nuc_to_pc_path(nuc_path):
    """Converts linux/nuc paths to windows/pc paths using global mapping."""
    n_lower = nuc_path.lower()
    for nuc_prefix, pc_prefix in NUC_PC_PREFIX_MAP.items():
        if n_lower.startswith(nuc_prefix.lower()):
            return os.path.normpath(pc_prefix + nuc_path[len(nuc_prefix):])
    return nuc_path


def create_plex_track_map(plex_tracks):
    """
    Creates a dictionary mapping NORMALIZED file paths to Plex track data.
    Key: Sanitized PC Path -> Value: {ratingKey, data, pc_path}
    """
    print(f'Mapping {len(plex_tracks)} Plex entries...', end=' ', flush=True)
    t_start = time.time()
    track_map = {}

    for track in plex_tracks:
        for media in track['Media']:
            for part in media['Part']:
                nuc_filepath = os.path.normpath(part['file'])
                pc_filepath = convert_nuc_to_pc_path(nuc_filepath)

                norm_key = uni.sanitize_path_for_matching(pc_filepath)
                track_map[norm_key] = {
                    'pc_path': pc_filepath,
                    'data': track,
                    'ratingKey': track['ratingKey']
                }

    print(
        f"Done in {time.time() - t_start:.2f}s. (Map Size: {len(track_map)})")
    return track_map


def is_whitelisted(path):
    """Checks if a path is in the allowed duplicate whitelist."""
    clean_path = uni.slugify(path, strip_non_alphanum=True)
    for ok in MULTIPLE_OK:
        if clean_path.startswith(uni.slugify(ok, strip_non_alphanum=True)):
            return True
    return False


def read_musicbee_playlists(input_dir, plex_map, counters, verbose=True):
    """
    Scans a directory for M3U files, resolves paths against the provided plex_map.
    Ignores paths found in MB_PATHS_TO_IGNORE.
    Returns a dictionary: {playlist_title: [list_of_rating_keys]}
    """
    uni.print_section('READING MUSICBEE PLAYLISTS')
    t_start = time.time()

    if not os.path.exists(input_dir):
        print(f"Warning: Playlist directory not found: {input_dir}")
        return {}

    sync_plex_playlists = {}
    m3u_files = [
        os.path.join(r, f)
        for r, d, f in os.walk(input_dir)
        for f in f
        if f.lower().endswith(('.m3u', '.m3u8'))
    ]

    print(f"Found {len(m3u_files)} playlists. Resolving paths...", flush=True)

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
            if not os.path.isabs(raw_path):
                full_pc_path = os.path.normpath(
                    os.path.join(os.path.dirname(filepath), raw_path))
            else:
                full_pc_path = os.path.normpath(raw_path)

            # 2. Check Ignore List
            is_ignored = False
            for ignore_path in MB_PATHS_TO_IGNORE:
                if full_pc_path.lower().startswith(ignore_path.lower()):
                    ignored_count += 1
                    is_ignored = True
                    break
            if is_ignored:
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

        if len(valid_keys) > MAX_PLEX_PLAYLIST_SIZE:
            counters['Playlist: Skipped (Too Large)'] += 1
            if verbose:
                print(
                    f"    [Skip] '{playlist_name}' too large: {len(valid_keys)} tracks (Limit: {MAX_PLEX_PLAYLIST_SIZE})")
        elif valid_keys:
            sync_plex_playlists[playlist_name] = valid_keys
        else:
            counters['Playlist: Skipped (Empty/All Ignored)'] += 1

        if verbose:
            info = f"'{playlist_name}' -> {len(valid_keys)} found"
            if missing_count > 0:
                info += f", {missing_count} missing"
            if ignored_count > 0:
                info += f", {ignored_count} ignored"
            print(info)

            effective_total = total_tracks - ignored_count
            if effective_total > 5 and (missing_count / effective_total) > 0.2:
                print(
                    f"    [Warn] High missing track count for '{playlist_name}'")

    print(f"Playlist processing finished in {time.time() - t_start:.2f}s.")
    return sync_plex_playlists


# =============================================================================
# MAIN EXECUTION
# =============================================================================
if __name__ == "__main__":
    t_total = time.time()

    # 1. Initialize Plex
    uni.print_section('INITIALIZING PLEX')
    t_init = time.time()
    plex_music = PlexMusic(PLEX_SERVER_URL, PLEX_TOKEN,
                           library_name=PLEX_LIBRARY_NAME)
    if SHOW_PLEX_INFO:
        plex_music.display_libraries()
    print(f"Plex connected in {time.time() - t_init:.2f}s.")

    # 2. Get Recent Tracks
    print(f"\nFetching Top {RECENTLY_RATED_LIMIT} Recently Rated tracks...")
    t_recent = time.time()
    recent_tracks = plex_music.get_recently_rated_tracks(
        limit=RECENTLY_RATED_LIMIT)
    recent_rating_keys = set([str(t['ratingKey']) for t in recent_tracks])
    print(
        f"Identified {len(recent_rating_keys)} recent tracks in {time.time() - t_recent:.2f}s.")

    if SHOW_RECENT_RATED:
        print('\n[Plex] Recently Rated:')
        plex_music.display_tracks(
            recent_tracks[:N_RECENT_DISPLAY], show_details=True)

    if SHOW_RECENT_ADDED:
        print('\n[Plex] Recently Added:')
        try:
            pl_tracks = plex_music.get_playlist_tracks('Recently Added')
            plex_music.display_tracks(
                pl_tracks[:N_RECENT_DISPLAY], show_details=True)
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
        MUSICBEE_LIBRARY_TSV, MUSICBEE_INBOX_TSV, save_tsv=False)
    mb_df['Path_Norm'] = mb_df['Path'].apply(uni.sanitize_path_for_matching)
    mb_df = uni.mb_standardize_ratings(mb_df)
    print(f'Loaded {len(mb_df)} tracks in {time.time() - t_mb:.2f}s.')
    mb_grouped = mb_df.groupby('Path_Norm')

    # 6. Main Sync Loop (Ratings)
    export_musicbee_playlists = []
    uni.print_section('SYNCING RATINGS')
    print(f"Starting Sync... (DRY RUN: {DRY_RUN})")
    t_sync = time.time()

    for i, (plex_key, plex_val) in enumerate(plex_map.items()):
        if i % PLEX_SYNC_LOG_EVERY == 0:
            print(f'{i/len(plex_map):0.0%} processed...', flush=True)
        plex_data, plex_path = plex_val['data'], plex_val['pc_path']
        track_title = plex_data.get('title', '')
        rating_key = str(plex_val['ratingKey'])

        # Metadata Fixes
        if not track_title or track_title.strip().lower() == "blank":
            counters['Metadata: Fixed from Filename'] += 1

        # Match Logic
        if plex_key not in mb_grouped.groups:
            counters['No Match (Plex Track not in MB)'] += 1
            continue
        mb_rows = mb_grouped.get_group(plex_key)
        mb_track_data = mb_rows.iloc[0]

        # Duplicate Detection
        if len(mb_rows) > 1:
            if is_whitelisted(plex_path):
                counters['Match (Duplicate Allowed)'] += 1
            else:
                counters['Match (Duplicate Warning)'] += 1
                if VERBOSE:
                    print(f"WARN: Multiple matches for {plex_path}")
        else:
            counters['Match Found'] += 1

        # Rating Sync
        p_rating = (float(plex_data.get('userRating', 0)) / 2.0)
        m_rating = float(mb_track_data['Rating']) if pd.notna(
            mb_track_data['Rating']) else 0.0

        # Cleanup Invalid Plex Ratings
        if p_rating > 0 and (p_rating % 0.5 != 0):
            if VERBOSE:
                print(
                    f"  CLEANUP: {track_title} (Plex: {p_rating}) -> Unrated [Invalid Rating]")
            if not DRY_RUN:
                plex_music.get_track(rating_key).rate(0)
                time.sleep(PLEX_SLEEP_TIME_S)
            counters['Plex Force Clean (Invalid)'] += 1
            p_rating = 0.0

        # Conflict Resolution
        if abs(p_rating - m_rating) > 0.1:
            if rating_key in recent_rating_keys and p_rating > 0:
                # Plex is Newer -> Export M3U
                export_musicbee_playlists.append(
                    {'Path': mb_track_data['Path'], 'Rating': p_rating})
                counters['Conflict: Recent Plex Rating (Export M3U)'] += 1
                if VERBOSE:
                    print(
                        f"  [Mismatch]: Add {track_title} to rating .m3u (Plex: {p_rating} | MB: {m_rating})")
            else:
                # MB is Newer/Master -> Overwrite Plex
                if not DRY_RUN:
                    plex_music.get_track(rating_key).rate(int(m_rating * 2))
                    counters['Plex Updated (MB Trumps)'] += 1
                    time.sleep(PLEX_SLEEP_TIME_S)
                else:
                    counters['Dry Run: MB Overwrite'] += 1
                    if VERBOSE:
                        print(
                            f"  [DRY] MB Overwrite: {track_title} (Plex: {p_rating} -> {m_rating})")
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
        existing_playlists_map = {
            p.title: p for p in plex_music.get_playlists()}
        for name, keys in sync_plex_playlists.items():
            if not DRY_RUN:
                existing_pl = existing_playlists_map.get(name)
                t_single = time.time()
                if plex_music.sync_playlist(name, keys, existing_playlist=existing_pl):
                    counters['Playlist: Created/Updated'] += 1
                    if VERBOSE:
                        print(
                            f"  [Sync] '{name}' ({len(keys)} tracks) in {time.time() - t_single:.1f}s", flush=True)
                else:
                    counters['Playlist: Failed'] += 1
                    if VERBOSE:
                        print(
                            f"  [Fail] '{name}' failed after {time.time() - t_single:.1f}s", flush=True)
            else:
                counters['Playlist: Dry Run Skip'] += 1
        print(f"Playlist sync finished in {time.time() - t_pl_sync:.2f}s.")

        # Playlist Cleanup
        uni.print_section('CLEANING UP SMALL PLAYLISTS')
        print("Checking for playlists with fewer than",
              f"{MIN_PLAYLIST_SIZE} tracks...")
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
            uni.save_m3u(os.path.join(RATING_EXPORT_DIR, fname), group['Path'].tolist())
            print(f"  -> {fname}: {len(group)} tracks")
    print(f'\nDone in {(time.time() - t_total)/60:0.1f}m.')

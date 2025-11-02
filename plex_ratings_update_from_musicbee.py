import time
import os
import sys
import pandas as pd
from collections import defaultdict

from plex_lib import PlexMusic
import unify_lib as uni
from auth import PLEX_TOKEN, PLEX_SERVER_URL

# CONFIGURATION
sys.stdout.reconfigure(encoding='utf-8')

PLEX_MUSIC_LIB = 'Music'
VERBOSE = True
DRY_RUN = False
PLEX_SYNC_LOG_EVERY = 5000
PLEX_SLEEP_TIME_S = 0.05
# Number of recently rated Plex tracks to save (Export M3U instead of Overwrite)
N_RECENT_PLEX_RATED = 100

# UI Options
SHOW_PLEX_LIBS = False
SHOW_RECENT_RATED = True
SHOW_RECENT_ADDED = True
N_RECENT_DISPLAY = 20

# Paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
M3U_DIR = os.path.join(SCRIPT_DIR, 'm3u')
LOG_DIR = os.path.join(SCRIPT_DIR, 'logs')
MB_LIB = os.path.join(SCRIPT_DIR, 'db_assets', 'musicbee_library.tsv')
MB_INBOX = os.path.join(SCRIPT_DIR, 'db_assets', 'musicbee_inbox.tsv')

# MAPPING & WHITELIST
NUC_PC_PREFIX_MAP = {
    '\\hdd\\seed\\': 'B:\\seed\\',
    '\\hdd\\seed_archive\\': 'B:\\seed_archive\\',
    '\\mnt\\music\\': 'D:\\Music\\',
}

MULTIPLE_OK = [
    # 123 / Symbols
    "b:\\seed_archive\\music_library\\1000 Gecs",
    "b:\\seed_archive\\music\\∩╝Æ∩╝ÿ∩╝æ∩╝ö - µû░πüùπüäµùÑπü«Φ¬òτöƒ",
    # A
    "b:\\seed_archive\\music_library\\Altin G├╝n - A┼ƒk",
    "b:\\seed_archive\\music_library\\Andre╠ü 3000 - New Blue Sun",
    "d:\\Music\\Library\\Aphex Twin\\Selected Ambient Works, Volume II",
    "b:\\seed_archive\\music\\Azmari - Sam─ü'─½",
    "b:\\seed_archive\\music\\A Winged Victory for the Sullen",
    # B
    "b:\\seed_archive\\music\\Babyshambles - Down In Albion",
    "b:\\seed_archive\\music_library\\BADBADNOTGOOD - Late Night Tales",
    "b:\\seed_archive\\music\\LateNightTales - Belle & Sebastian Vol. 2",
    "b:\\seed_archive\\music_library\\Bj├╢rk - Debut",
    "b:\\seed_archive\\music_library\\Bj├╢rk - fossora",
    "b:\\seed_archive\\music_library\\Bj├╢rk -2015- Vulnicura",
    "b:\\seed_archive\\music_library\\Boris - Smile",
    "b:\\seed_archive\\music_library\\Burzum - Filosofem",
    # D
    "b:\\seed_archive\\music\\Dauwd - 2013 - Heat Division Remixes",
    "b:\\seed_archive\\music_library\\David Bowie - Heroes",
    "b:\\seed_archive\\music_library\\Django Reinhardt - Djangology",
    # F
    "b:\\seed_archive\\music_library\\Fleet Foxes - Crack-Up",
    "b:\\seed_archive\\music\\Fran├ºoise Hardy - Comment te dire adieu",
    # G
    "b:\\seed_archive\\music_library\\Glass Animals - Remixes",
    # H
    "b:\\seed_archive\\music\\Helado Negro - Far In",
    "b:\\seed_archive\\music_library\\Hot Sugar - Seductive Nightmares 2",
    # J
    "b:\\seed_archive\\music_library\\Janelle Mon├íe - Dirty Computer",
    "b:\\seed_archive\\music_library\\Jean Michel Jarre - 1976 - Oxygene",
    # K
    "b:\\seed_archive\\music_library\\Keith Jarrett - The Ko╠êln Concert",
    "b:\\seed_archive\\music\\Kruder & Dorfmeister - The K&D Sessions",
    # L
    "b:\\seed_archive\\music\\L'Impe╠üratrice - Tako Tsubo",
    "b:\\seed_archive\\music\\Laryssa Kim - Contezza",
    "b:\\seed_archive\\music_library\\The Lions - Soul Riot",
    "b:\\seed_archive\\music\\Los Lobos - How Will the Wolf Survive",
    "b:\\seed_archive\\music_lossy\\Luc Ferrari - Presque Rien",
    "b:\\seed_archive\\music_library\\Lusine - Sensorimotor",
    "b:\\seed_archive\\music\\Lykke Li - So Sad So Sexy",
    # M
    "b:\\seed_archive\\music\\Mach-Hommy - Pray For Haiti",
    "b:\\seed_archive\\music_library\\Madlib - Sound Ancestors",
    "b:\\seed_archive\\music\\Magma - Me╠êkani╠êk De╠êstrukti╠êw╠Ç Ko╠êmmando╠êh",
    # N
    "b:\\seed_archive\\music_library\\Nicolas Jaar - Cenizas",
    "b:\\seed_archive\\music_library\\Nicolas Jaar - Pomegranates",
    "b:\\seed_archive\\music_library\\Nosaj Thing - Continua",
    # P
    "b:\\seed_archive\\music\\ßäæßàíßäàßàíßå½ (Parannoul)",
    "b:\\seed_archive\\music_library\\Plat - Compulsion",
    "b:\\seed_archive\\music\\Potatoi - Toy",
    # R
    "b:\\seed_archive\\music\\Rammstein - Zeit",
    "b:\\seed_archive\\music_library\\Ricky Eat Acid - You get sick",
    "b:\\seed_archive\\music_library\\R├╢yksopp - Profound Mysteries",
    # S
    "b:\\seed_archive\\music_library\\Santana - Supernatural",
    "b:\\seed_archive\\music\\ScHoolboy Q - Setbacks",
    "b:\\seed_archive\\music_library\\Skinshape - Umoja",
    "b:\\seed_archive\\music\\Sofia Kourtesis - Madres",
    "b:\\seed_archive\\music_library\\Sonic Youth - 1992 - Dirty",
    "b:\\seed_archive\\music_library\\Stan Getz - 1976 - The Best of Two Worlds",
    "b:\\seed_archive\\music_library\\Stan Getz & Jo├úo Gilberto - Getz-Gilberto",
    "b:\\seed_archive\\music\\Sunn O))) - Monoliths & Dimensions",
    # T
    "b:\\seed_archive\\music\\Takeuchi, Mariya - Variety",
    "b:\\seed_archive\\music_library\\Thriftworks - Rainmaker",
    "b:\\seed_archive\\music_library\\Todd Terje - It's Album Time",
    # V
    "b:\\seed_archive\\music_library\\Vieux Farka Tour├⌐ & Khruangbin - Ali",
    # Y / Z
    "b:\\seed_archive\\music_library\\Young Montana - Limerence",
    "b:\\seed_archive\\music_lossy\\Yumi Zouma (2014) - Yumi Zouma",
]


def convert_nuc_to_pc_path(nuc_path):
    """Converts linux/nuc paths to windows/pc paths."""
    for nuc_prefix, pc_prefix in NUC_PC_PREFIX_MAP.items():
        if nuc_path.startswith(nuc_prefix):
            return os.path.normpath(pc_prefix + nuc_path[len(nuc_prefix):])
    return nuc_path


def create_plex_track_map(plex_tracks):
    """Creates a dictionary mapping NORMALIZED file paths to Plex track data."""
    print('\nMapping Plex entries...', end=' ', flush=True)
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
    print(f"Created track map with {len(track_map)} entries")
    return track_map


def load_musicbee_data():
    """Ingests and sanitizes MusicBee library."""
    print('\n' + '='*40 + '\n LOADING MUSICBEE DB \n' + '='*40)
    df = uni.ingest_musicbee_db_assets(MB_LIB, MB_INBOX, save_tsv=False)
    print('Sanitizing paths...', end=' ', flush=True)
    df['Path_Norm'] = df['Path'].apply(uni.sanitize_path_for_matching)
    print('Done.')
    df = uni.mb_standardize_ratings(df)
    print(f'Loaded {len(df)} tracks.')
    return df.groupby('Path_Norm')


def is_whitelisted(path):
    """Checks if a path is in the MULTIPLE_OK whitelist."""
    clean_path = uni.slugify(path, strip_non_alphanum=True)
    for ok in MULTIPLE_OK:
        if clean_path.startswith(uni.slugify(ok, strip_non_alphanum=True)):
            return True
    return False


# MAIN
if __name__ == "__main__":
    t0 = time.time()

    # 1. Initialize Plex
    print('\n' + '='*40 + '\n INITIALIZING PLEX \n' + '='*40)
    plex_music = PlexMusic(PLEX_SERVER_URL, PLEX_TOKEN)
    if SHOW_PLEX_LIBS:
        plex_music.display_libraries()

    # 2. Get Recent Tracks
    print(
        f"Fetching Top {N_RECENT_PLEX_RATED} Recently Rated tracks to prevent overwrites...")
    recent_tracks = plex_music.get_recently_rated_tracks(
        limit=N_RECENT_PLEX_RATED)
    recent_keys = set([str(t['ratingKey']) for t in recent_tracks])
    print(f"Identified {len(recent_keys)} recent tracks.")

    # 3. Display Stats
    if SHOW_RECENT_RATED:
        print('\n[Plex] Recently Rated:')
        plex_music.display_tracks(
            recent_tracks[:N_RECENT_DISPLAY], show_details=True)

    # 4. Create Plex Map
    plex_tracks = plex_music.get_tracks()
    print(f'Fetched {len(plex_tracks)} tracks total.')
    plex_map = create_plex_track_map(plex_tracks)

    # 5. Load MB Data
    mb_grouped = load_musicbee_data()

    # 6. Init Counters
    counters = defaultdict(int)
    m3u_counts = defaultdict(int)
    export_queue = []
    print('\n' + '='*40 + '\n SYNCING \n' + '='*40)
    print(f"Starting Sync... (DRY RUN: {DRY_RUN})")

    # 7. Main Sync Loop
    for i, (plex_key, plex_val) in enumerate(plex_map.items()):
        if i % PLEX_SYNC_LOG_EVERY == 0:
            print(f'{i/len(plex_map):0.0%} processed...', flush=True)
        plex_data, plex_path = plex_val['data'], plex_val['pc_path']
        track_title = plex_data.get('title', '')
        rating_key = str(plex_val['ratingKey'])
        # BLANK TITLE FALLBACK
        # If title is missing or "Blank", fallback to filename
        if not track_title or track_title.strip().lower() == "blank":
            # Use filename as title (e.g., '01 - Song Name')
            filename = os.path.basename(plex_path)
            track_title = os.path.splitext(filename)[0]
            counters['Metadata: Fixed from Filename'] += 1
            # Optional: don't print every single one if you have many, but useful for now
            # if VERBOSE:
            #     print(f"Metadata Fix: Using filename for '{filename}'")

        # MATCHING LOGIC
        if plex_key not in mb_grouped.groups:
            counters['No Match'] += 1
            continue
        mb_rows = mb_grouped.get_group(plex_key)
        mb_match = mb_rows.iloc[0]
        if len(mb_rows) > 1:
            if is_whitelisted(plex_path):
                counters['Match (Duplicate Allowed)'] += 1
            else:
                counters['Match (Duplicate Warning)'] += 1
                if VERBOSE:
                    print(f"WARN: Multiple matches for {plex_path}")
        else:
            counters['Match Found'] += 1

        # SYNC LOGIC
        p_rating = (float(plex_data.get('userRating', 0)) / 2.0)
        m_rating = float(mb_match['Rating']) if pd.notna(
            mb_match['Rating']) else 0.0

        # 1. Cleanup Invalid Ratings (e.g. 1.7, 3.8) - Reset to Unrated
        if p_rating > 0 and (p_rating % 0.5 != 0):
            if VERBOSE:
                print(
                    f"  CLEANUP: {track_title} (Plex: {p_rating}) -> Unrated [Invalid Rating]")
            if not DRY_RUN:
                plex_music.get_track(rating_key).rate(0)
                time.sleep(PLEX_SLEEP_TIME_S)
            counters['Plex Force Clean (Invalid)'] += 1
            p_rating = 0.0  # Local update for subsequent logic

        # 2. Conflict Resolution
        if abs(p_rating - m_rating) > 0.1:
            is_recent = rating_key in recent_keys
            # A. Recent Plex Rating -> Export M3U (Do NOT overwrite)
            if is_recent and p_rating > 0:
                export_queue.append(
                    {'Path': mb_match['Path'], 'Rating': p_rating})
                m3u_counts[p_rating] += 1
                counters['Conflict: Recent Plex Rating (Export M3U)'] += 1
                if VERBOSE:
                    print(
                        f"  M3U EXPORT: {track_title} (Plex: {p_rating} | MB: {m_rating}) [Recent Plex Rating]")

            # B. Older/Stale Rating -> MusicBee Overwrite Plex
            else:
                if DRY_RUN:
                    counters['Dry Run: MB Overwrite'] += 1
                    if VERBOSE:
                        print(
                            f"  [DRY] MB Overwrite: {track_title} (Plex: {p_rating} -> {m_rating})")
                else:
                    plex_music.get_track(rating_key).rate(int(m_rating * 2))
                    counters['Plex Updated (MB Trumps)'] += 1
                    time.sleep(PLEX_SLEEP_TIME_S)
        else:
            counters['Ratings Match'] += 1
            if p_rating > 0:
                counters['Total Rated Matches'] += 1
            else:
                counters['Matched (Unrated)'] += 1

    # 8. Final Report
    print("\n" + "-"*30 + "\n SYNC SUMMARY \n" + "-"*30)
    for k, v in sorted(counters.items()):
        print(f"{k:<25}: {v}")

    # Export M3Us
    if export_queue:
        print(f"\nWriting M3U Exports to '{M3U_DIR}'...")
        if not os.path.exists(M3U_DIR):
            os.makedirs(M3U_DIR)
        df = pd.DataFrame(export_queue)
        for rating, group in df.groupby('Rating'):
            fname = f"plex_ratings_{rating}.m3u"
            group['Path'].to_csv(os.path.join(
                M3U_DIR, fname), sep='\t', header=False, index=False)
            print(f"  -> {fname}: {len(group)} tracks")
            
    print(f'\nDone in {(time.time() - t0)/60:0.1f}m.')

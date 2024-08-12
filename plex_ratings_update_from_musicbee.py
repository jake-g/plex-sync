
import time
import os
from collections import defaultdict
import pandas as pd

from plex_lib import PlexMusic
import unify_lib as uni

# Your Plex server details and credentials
from auth import PLEX_TOKEN, PLEX_SERVER_URL
PLEX_MUSIC_LIB = 'Music'  # Name of the Music library section
# Plex Sync
VERBOSE = False
PLEX_SYNC_LOG_EVERY = 10000
PLEX_SLEEP_TIME_S = 0.1

# Options
SHOW_PLEX_LIBS = False
SHOW_PLEX_PLAYLISTS = False
SHOW_RECENT_RATED = True
SHOW_RECENT_ADDED = True
N_RECENT = 15

# Inputs
DATE = time.strftime('%m-%d-%Y')
CACHE_FOLDER = 'logs'

# MusicBee Database
MB_LIB = './db_assets/musicbee_library.tsv'
MB_INBOX = './db_assets/musicbee_inbox.tsv'


MULTIPLE_OK = [
    "b:\\seed_archive\\music_library\\andre 3000 - new blue sun (2023) - web flac\\",
    "b:\\seed_archive\\music_library\\bjork - debut (flac, log, cue)\\",
    "b:\\seed_archive\\music_library\\bjork - fossora (2022) [cd flac] {tplp1485cd2}\\",
    "b:\\seed_archive\\music_library\\janelle monae - dirty computer (2018) - cd flac\\",
    "b:\\seed_archive\\music_library\\magma - mekanik destruktiw kommandoh (1973) [flac] (1989 seventh records rex vii)\\",
    "b:\\seed_archive\\music_library\\royksopp - profound mysteries (2022) {dog051cd cd} [flac]\\",
    "b:\\seed_archive\\music_library\\stan getz & joao gilberto - getz-gilberto [verve records japan pocj-9202]\\",
    "b:\\seed_archive\\music_library\\vieux farka toure & khruangbin - ali (2022) - web flac\\",
    "b:\\seed_archive\\music_library\\boris - smile [jp] [cd] [flac]\\",
    "b:\\seed_archive\\music_library\\david bowie - heroes (1984) - rca pd83857 germany - flac\\",
    "b:\\seed_archive\\music_library\\skinshape - umoja (2020) - web flac\\",
    "b:\\seed_archive\\music_library\\santana - supernatural (1999) (07822-19080-2) [cd flac]\\",
    "b:\\seed_archive\\music_library\\django reinhardt - djangology (2002) {bluebird 09026-63957-2} [flac-cd]\\",
    "b:\\seed_archive\\music_library\\nosaj thing - continua (2022) - web flac\\",
    "b:\\seed_archive\\music_library\\nicolas jaar - pomegranates (2015) web flac\\",
    "b:\\seed_archive\\music_library\\nicolas jaar - cenizas (2020) [web flac]\\",
    "b:\\seed_archive\\music_library\\madlib - sound ancestors (2021) - web flac\\",
    "b:\\seed_archive\\music_library\\lusine - sensorimotor (2017) [flac]\\",
    "b:\\seed_archive\\music_library\\various artists - late night tales khruangbin (2020) - web flac\\",
    "b:\\seed_archive\\music_library\\fleet foxes - crack-up (2017) [flac]\\",
    "b:\\seed_archive\\music_library\\plat - compulsion (2005) - de-emphasized flac\\",
    "b:\\seed_archive\\music\\dauwd - 2013 - heat division remixes [flac]\\",
    "b:\\seed_archive\\music\\l'imperatrice - tako tsubo (2021) - web flac\\",
    "b:\\seed_archive\\music\\ (parannoul) - 2021 - to see the next part of the dream [web flac]\\",
    "b:\\seed_archive\\music\\helado negro - far in (2021) - web flac\\",
]


NUC_PC_PREFIX_MAP = {
    '\\hdd\\seed\\': 'B:\\seed\\',
    '\\hdd\\seed_archive\\': 'B:\\seed_archive\\',
    '\\mnt\\music\\': 'D:\\Music\\',
}

def convert_nuc_to_pc_path(nuc_path, prefix_map=NUC_PC_PREFIX_MAP):
    """Converts a NUC path to a PC path using a prefix map."""
    for nuc_prefix, pc_prefix in prefix_map.items():
        if nuc_path.startswith(nuc_prefix):
            return os.path.normpath(pc_prefix + nuc_path[len(nuc_prefix):])
    print(f'WARNING: Path {nuc_path} prefix not in {prefix_map}')
    return nuc_path


def create_plex_track_map(plex_tracks):
    """Creates a dictionary mapping file paths to Plex track data."""
    track_map = {}
    for track in plex_tracks:
        nuc_filepath = os.path.normpath(track['Media'][0]['Part'][0]['file'])
        pc_filepath = convert_nuc_to_pc_path(nuc_filepath)
        track_map[pc_filepath] = track
    print(f"Created track map with {len(track_map)} entries")
    return track_map


if __name__ == "__main__":

  print('\nInitializing Plex Music Library...')
  t0 = time.time()
  plex_music = PlexMusic(PLEX_SERVER_URL, PLEX_TOKEN)

  if SHOW_PLEX_LIBS:
      plex_music.display_libraries()

  plex_playlists = plex_music.get_playlists()
  print(f'Fetched {len(plex_playlists)} {PLEX_MUSIC_LIB} playlists')
  plex_tracks = plex_music.get_tracks()
  print(f'Fetched {len(plex_tracks)} {PLEX_MUSIC_LIB} tracks')

  if SHOW_PLEX_PLAYLISTS:
      print('\nPlex Playlists...')
      plex_music.display_playlists()

  if SHOW_RECENT_RATED:
      print('\nPlex Recently Rated...')
      recently_rated = plex_music.get_recently_rated_tracks(limit=N_RECENT)
      print(f'Tracks recently Rated (showing {N_RECENT}):')
      plex_music.display_tracks(recently_rated, show_details=True)

  if SHOW_RECENT_ADDED:
      playlist_name = 'Recently Added'
      print(f'\nPlex {playlist_name}...')
      playlist_tracks = plex_music.get_playlist_tracks(playlist_name)
      print(f"Tracks from '{playlist_name}' Playlist (showing {N_RECENT}):")
      plex_music.display_tracks(playlist_tracks[:N_RECENT], show_details=True)

  print('\nInitializing Music Bee Database...')
  # Load Tracks to Rate
  mb_tracks = uni.ingest_musicbee_db_assets(MB_LIB, MB_INBOX, save_tsv=False)
  mb_tracks['Path_Norm'] = mb_tracks['Path'].apply(uni.slugify)
  mb_tracks = uni.mb_standardize_ratings(mb_tracks)
  mb_tracks_rated = mb_tracks.loc[mb_tracks['Rating'] > 0]
  mb_tracks_rated['Rating'].value_counts()
  print(f'Found {len(mb_tracks)} total mb tracks, {len(mb_tracks_rated)} of them',
        f'({len(mb_tracks_rated)/len(mb_tracks):0.1%}) are rated')
  mb_tracks_rated.info()

  print('\nMatch MusicBee and Plex Entries...')
  plex_track_map = create_plex_track_map(plex_music.get_tracks())
  # Normalize paths in mb_tracks
  mb_paths_norm = set(mb_tracks['Path_Norm'])
  # Normalize keys in plex_track_map (in-place modification)
  plex_track_map_norm = {uni.slugify(k): v for k, v in plex_track_map.items()}
  match_counters = defaultdict(int)
  plex_mb_map = {}
  ignore_match_path = 'd:\\music\\mixes\\'  # dont care
  for pc_filepath, plex_track in plex_track_map_norm.items():
      if pc_filepath in mb_paths_norm:
          matching_rows = mb_tracks[mb_tracks['Path_Norm'] == pc_filepath]
          if len(matching_rows) > 1:
              if not any(pc_filepath.startswith(ok_path) for ok_path in MULTIPLE_OK):
                  print(f'Warning: Multiple matches (taking first) for',
                        f'{pc_filepath}:\n{matching_rows["fuzzy_track_id"]}\n')
                  match_counters['Multiple Matches'] += 1
          match_counters['Match Found'] += 1
          mb_track_row = matching_rows.iloc[0]
          plex_mb_map[pc_filepath] = mb_track_row
      else:
          match_counters['No Matches'] += 1
          if ignore_match_path not in pc_filepath:
              print(f'Warning: No matches for {pc_filepath}')

  print("Match Counts:")
  for name, count in match_counters.items():
      print(f"{name}: {count}")

  print('\nUpdate Plex Rating from MusicBee Rating...')
  rated_tracks = []
  counters = defaultdict(int)
  mb_paths_rated = set(mb_tracks_rated['Path_Norm'])
  for i, (pc_filepath, plex_track_data) in enumerate(plex_track_map_norm.items(), start=1):
      if i % PLEX_SYNC_LOG_EVERY == 0:
          print(f'{i/len(plex_track_map_norm):0.0%}', end='...')
      if pc_filepath not in mb_paths_rated:
          counters['plex_track_not_in_mb_rated'] += 1
          continue

      # Get MusicBee rating and convert to Plex scale
      mb_row = mb_tracks_rated[mb_tracks_rated['Path_Norm']
                               == pc_filepath].iloc[0]
      new_plex_rating = None
      if pd.notna(mb_row['Rating']) and 0 < float(mb_row['Rating']) <= 5:
          new_plex_rating = int(round(mb_row['Rating'] * 2.0))

      cur_plex_rating = plex_track_data.get('userRating', None)
      if cur_plex_rating == new_plex_rating:
          counters['plex_track_rating_matches'] += 1
      else:
          rated_tracks.append(plex_track_data)
          track = plex_music.get_track(plex_track_data["ratingKey"])
          track.rate(new_plex_rating)
          time.sleep(PLEX_SLEEP_TIME_S)
          if VERBOSE:
              print(f"Updating rating from {cur_plex_rating} to {
                    new_plex_rating} for {pc_filepath}")
          if cur_plex_rating != None:
              counters['plex_track_rating_updated'] += 1
          else:
              counters['plex_track_rating_set'] += 1

  print("Plex Rating Counts:")
  for name, count in counters.items():
      print(f"{name}: {count}")

  print(f"\nSaving Map and Counters to {CACHE_FOLDER}")
  uni.save_dict_to_file(
      plex_mb_map,
      os.path.join(CACHE_FOLDER, f'plex_musicbeemap.tsv'),
  )
  uni.save_dict_to_file(
      dict(match_counters),
      os.path.join(CACHE_FOLDER, f'plex_musicbee_match_counters_{DATE}.tsv'),
  )
  uni.save_dict_to_file(
      dict(counters),
      os.path.join(
          CACHE_FOLDER, f'plex_rating_from_musicbee_counters_{DATE}.tsv'),
  )

  print(f'\nFinished in {(time.time() - t0)/60:0.1f} minutes')

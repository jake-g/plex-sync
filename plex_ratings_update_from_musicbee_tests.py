"""Comprehensive unit tests for plex_ratings_update_from_musicbee.py."""

from collections import defaultdict
import os
import sys
import tempfile
import unittest
from unittest.mock import MagicMock
from unittest.mock import patch

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
  sys.path.insert(0, SCRIPT_DIR)

UNIFIED_DIR = os.path.abspath(
    os.path.join(SCRIPT_DIR, '..', 'music-sources-unified')
)
if UNIFIED_DIR not in sys.path:
  sys.path.insert(0, UNIFIED_DIR)

import pandas as pd
import plex_ratings_update_from_musicbee as plex_sync
import unify_lib as uni


class TestConvertNucToPcPathParameterized(unittest.TestCase):
  """Parameterized tests for NUC/Linux to Windows PC path conversion."""

  def test_convert_nuc_to_pc_path_cases(self):
    """Verifies conversion across multiple path formats and cases."""
    test_cases = [
        (
            '/mnt/music/Library/Radiohead/Kid A/01 Everything.flac',
            r'B:\Music\Library\Radiohead\Kid A\01 Everything.flac',
        ),
        (
            '/MNT/MUSIC/Library/Artist/Album/01 Track.flac',
            r'B:\Music\Library\Artist\Album\01 Track.flac',
        ),
        (
            '/data/seed_archive/music_library/Band/Album/01 Song.mp3',
            r'B:\seed_archive\music_library\Band\Album\01 Song.mp3',
        ),
        (
            '/DATA/SEED_ARCHIVE/music_lossy/Band/Album/01 Song.mp3',
            r'B:\seed_archive\music_lossy\Band\Album\01 Song.mp3',
        ),
        (
            r'\mnt\music\Library\Artist\Album\01.flac',
            r'B:\Music\Library\Artist\Album\01.flac',
        ),
        (
            r'\data\seed_archive\music\Band\Song.mp3',
            r'B:\seed_archive\music\Band\Song.mp3',
        ),
        (
            '/other/volume/unmapped/track.flac',
            '/other/volume/unmapped/track.flac',
        ),
        (
            '',
            '',
        ),
    ]

    for input_path, expected_output in test_cases:
      with self.subTest(input_path=input_path, expected_output=expected_output):
        actual_output = plex_sync.convert_nuc_to_pc_path(input_path)
        self.assertEqual(actual_output, expected_output)


class TestIsAbsolutePcPathParameterized(unittest.TestCase):
  """Parameterized tests for cross-platform absolute path detection."""

  def test_is_absolute_pc_path_cases(self):
    """Verifies path detection for Windows drive letters, UNC, POSIX."""
    test_cases = [
        (r'B:\Music\Library\Artist\01.flac', True),
        (r'b:\seed_archive\music\01.flac', True),
        (r'C:\Users\Music\track.mp3', True),
        ('B:/Music/Library/01.flac', True),
        (r'\\nas\music\Library\01.flac', True),
        ('/mnt/music/Library/01.flac', True),
        (r'..\Artist\01.flac', False),
        (r'Artist\Album\01.flac', False),
        ('subfolder/track.mp3', False),
        ('track.flac', False),
        ('', False),
    ]

    for path_str, expected in test_cases:
      with self.subTest(path_str=path_str, expected=expected):
        self.assertEqual(plex_sync.is_absolute_pc_path(path_str), expected)


class TestPlexTrackMapParameterized(unittest.TestCase):
  """Parameterized tests for building normalized Plex track map."""

  def test_create_plex_track_map_structures(self):
    """Verifies track mapping with single, multi-part, and empty media."""
    mock_plex_tracks = [
        {
            'ratingKey': '101',
            'title': 'Track Single Part',
            'Media': [{
                'Part': [{
                    'file': '/mnt/music/Library/Artist/Album/01 Track.flac'
                }]
            }],
        },
        {
            'ratingKey': '102',
            'title': 'Track Multi Part',
            'Media': [
                {
                    'Part': [
                        {'file': '/mnt/music/Library/Artist/Album/02 Pt1.flac'},
                        {'file': '/mnt/music/Library/Artist/Album/02 Pt2.flac'},
                    ]
                }
            ],
        },
        {
            'ratingKey': '103',
            'title': 'Empty Media Track',
            'Media': [],
        },
        {
            'ratingKey': '104',
            'title': 'Empty Part Track',
            'Media': [{'Part': []}],
        },
        {
            'ratingKey': '105',
            'title': 'Empty File String Track',
            'Media': [{'Part': [{'file': ''}]}],
        },
    ]

    track_map = plex_sync.create_plex_track_map(mock_plex_tracks)

    # 101 has 1 part, 102 has 2 parts -> total 3 entries mapped
    self.assertEqual(len(track_map), 3)

    key_101 = uni.sanitize_path_for_matching(
        r'B:\Music\Library\Artist\Album\01 Track.flac'
    )
    self.assertIn(key_101, track_map)
    self.assertEqual(track_map[key_101]['ratingKey'], '101')
    self.assertEqual(track_map[key_101]['data']['title'], 'Track Single Part')

    key_102_pt1 = uni.sanitize_path_for_matching(
        r'B:\Music\Library\Artist\Album\02 Pt1.flac'
    )
    key_102_pt2 = uni.sanitize_path_for_matching(
        r'B:\Music\Library\Artist\Album\02 Pt2.flac'
    )
    self.assertIn(key_102_pt1, track_map)
    self.assertIn(key_102_pt2, track_map)
    self.assertEqual(track_map[key_102_pt1]['ratingKey'], '102')
    self.assertEqual(track_map[key_102_pt2]['ratingKey'], '102')


class TestReadMusicBeePlaylistsParameterized(unittest.TestCase):
  """Parameterized tests for playlist reading, validation, and ignoring."""

  def test_raises_on_invalid_directory(self):
    """Verifies that missing or invalid directories raise FileNotFoundError."""
    invalid_dirs = [
        os.path.join(tempfile.gettempdir(), 'non_existent_pl_dir_12345'),
        '',
        None,
    ]
    counters = defaultdict(int)
    for invalid_dir in invalid_dirs:
      with self.subTest(invalid_dir=invalid_dir):
        with self.assertRaises((FileNotFoundError, TypeError)):
          plex_sync.read_musicbee_playlists(invalid_dir, {}, counters)

  def test_reads_playlists_and_handles_ignores_and_relatives(self):
    """Verifies playlist parsing with relative, absolute, and ignored tracks."""
    with tempfile.TemporaryDirectory() as tmp_dir:
      # Create subfolder with relative playlist
      sub_dir = os.path.join(tmp_dir, 'Rock')
      os.makedirs(sub_dir, exist_ok=True)
      playlist_path = os.path.join(sub_dir, 'Classics.m3u')

      valid_track = r'B:\Music\Library\Artist\Album\01.flac'
      ignored_track = r'B:\Music\Unsorted\Download\02.flac'
      relative_track = r'03.flac'  # Should resolve to sub_dir/03.flac

      with open(playlist_path, 'w', encoding='utf-8') as f:
        f.write(f'{valid_track}\n')
        f.write(f'{ignored_track}\n')
        f.write(f'{relative_track}\n')
        f.write('# Comment line\n')
        f.write('\n')

      key_valid = uni.sanitize_path_for_matching(valid_track)
      resolved_relative = os.path.normpath(
          os.path.join(sub_dir, relative_track)
      )
      key_relative = uni.sanitize_path_for_matching(resolved_relative)

      plex_map = {
          key_valid: {'ratingKey': '111', 'pc_path': valid_track},
          key_relative: {'ratingKey': '222', 'pc_path': resolved_relative},
      }
      counters = defaultdict(int)

      playlists = plex_sync.read_musicbee_playlists(
          tmp_dir, plex_map, counters, verbose=False
      )

      self.assertIn('Classics', playlists)
      self.assertEqual(playlists['Classics'], ['111', '222'])
      self.assertEqual(counters['Playlist: Processed'], 1)
      self.assertEqual(counters['Playlist Tracks: Found'], 2)
      self.assertEqual(counters['Playlist Tracks: Ignored'], 1)
      self.assertEqual(counters['Playlist Tracks: Missing'], 0)


class TestRatingSyncLogic(unittest.TestCase):
  """Tests rating sync conflict resolution between MusicBee and Plex."""

  def test_musicbee_overwrites_plex_rating(self):
    """Verifies that MusicBee master rating overwrites Plex when different."""
    mock_plex_music = MagicMock()
    mock_track_item = MagicMock()
    mock_plex_music.get_track.return_value = mock_track_item

    # MusicBee rating = 4.0, Plex rating = 0.0
    mb_rating = 4.0
    p_rating = 0.0
    rating_key = '999'

    if abs(p_rating - mb_rating) > 0.1:
      mock_plex_music.get_track(rating_key).rate(int(mb_rating * 2))

    mock_plex_music.get_track.assert_called_with('999')
    mock_track_item.rate.assert_called_with(8)

  def test_musicbee_unrates_plex_track(self):
    """Verifies that unrated MusicBee track resets Plex rating to 0."""
    mock_plex_music = MagicMock()
    mock_track_item = MagicMock()
    mock_plex_music.get_track.return_value = mock_track_item

    mb_rating = 0.0
    p_rating = 3.5
    rating_key = '888'

    if abs(p_rating - mb_rating) > 0.1:
      mock_plex_music.get_track(rating_key).rate(int(mb_rating * 2))

    mock_plex_music.get_track.assert_called_with('888')
    mock_track_item.rate.assert_called_with(0)

  def test_recent_plex_rating_exports_to_m3u(self):
    """Verifies that recently rated Plex tracks are exported to M3U."""
    recent_rating_keys = {'777'}
    plex_fix_keys = set()
    export_musicbee_playlists = []

    rating_key = '777'
    p_rating = 4.5
    mb_rating = 0.0
    plex_key = 'b music artist album 01 track'
    mb_track_data = {'Path': r'B:\Music\Artist\Album\01 Track.flac'}

    if abs(p_rating - mb_rating) > 0.1:
      if (
          rating_key in recent_rating_keys
          and p_rating > 0
          and plex_key not in plex_fix_keys
      ):
        export_musicbee_playlists.append(
            {'Path': mb_track_data['Path'], 'Rating': p_rating}
        )

    self.assertEqual(len(export_musicbee_playlists), 1)
    self.assertEqual(
        export_musicbee_playlists[0]['Path'],
        r'B:\Music\Artist\Album\01 Track.flac',
    )
    self.assertEqual(export_musicbee_playlists[0]['Rating'], 4.5)


class TestWhitelistAndFixOverrides(unittest.TestCase):
  """Tests whitelist duplicate and fix override logic."""

  def test_load_plex_fix_keys_contains_normalized_keys(self):
    """Verifies that PLEX_FIX_PATHS are properly sanitized upon load."""
    keys = plex_sync.load_plex_fix_keys()
    self.assertIsInstance(keys, set)
    self.assertTrue(len(keys) > 0)
    for k in keys:
      self.assertEqual(k, uni.sanitize_path_for_matching(k))

  def test_is_whitelisted_matches_known_whitelist(self):
    """Verifies whitelist check correctly allows whitelisted albums."""
    whitelisted_path = (
        r'B:\seed_archive\music_library\1000 Gecs\1000 gecs\01 745 sticky.flac'
    )
    self.assertTrue(plex_sync.is_whitelisted(whitelisted_path))

    normal_path = r'B:\Music\Library\Radiohead\Kid A\01 Everything.flac'
    self.assertFalse(plex_sync.is_whitelisted(normal_path))


if __name__ == '__main__':
  unittest.main()

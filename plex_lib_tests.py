"""Unit tests for plex_lib.py."""

from datetime import datetime
from datetime import timedelta
import unittest
from unittest.mock import MagicMock
from unittest.mock import patch

import plex_lib


class TestPlexMusicUtils(unittest.TestCase):
  """Tests for utility functions in PlexMusic."""

  @patch('plex_lib.PlexServer')
  def test_time_ago_in_days(self, mock_plex_server):
    """Test time_ago_in_days."""
    # Mock the PlexServer to avoid connection attempts
    mock_instance = MagicMock()
    mock_plex_server.return_value = mock_instance

    # Instantiate PlexMusic
    plex_music = plex_lib.PlexMusic('http://localhost:32400', 'token')

    # Current time
    now = datetime.now()
    # Timestamp for 5 days ago
    timestamp_5_days_ago = int((now - timedelta(days=5)).timestamp())

    # Test
    days_ago = plex_music.time_ago_in_days(timestamp_5_days_ago)
    self.assertEqual(days_ago, 5)

  @patch('plex_lib.PlexServer')
  def test_track_to_dict(self, mock_plex_server):
    """Test track_to_dict."""
    mock_instance = MagicMock()
    mock_plex_server.return_value = mock_instance

    plex_music = plex_lib.PlexMusic('http://localhost:32400', 'token')

    # Mock Track object
    mock_track = MagicMock()
    mock_track.title = "Song Title"
    mock_track.grandparentTitle = "Artist Name"
    mock_track.parentTitle = "Album Name"
    mock_track.parentYear = 2020
    mock_track.userRating = 5.0
    mock_track.viewCount = 10
    mock_track.lastRatedAt = datetime(2026, 1, 1)

    mock_media = MagicMock()
    mock_media.audioCodec = "flac"
    mock_track.media = [mock_media]

    # Test
    track_dict = plex_music.track_to_dict(mock_track)

    self.assertEqual(track_dict['title'], "Song Title")
    self.assertEqual(track_dict['grandparentTitle'], "Artist Name")
    self.assertEqual(track_dict['parentTitle'], "Album Name")
    self.assertEqual(track_dict['parentYear'], 2020)
    self.assertEqual(track_dict['userRating'], 5.0)
    self.assertEqual(track_dict['viewCount'], 10)
    self.assertEqual(
        track_dict['lastRatedAt'], datetime(2026, 1, 1).timestamp()
    )
    self.assertEqual(track_dict['Media'][0]['audioCodec'], "flac")


  @patch('plex_lib.PlexServer')
  def test_track_to_dict_missing_fields(self, mock_plex_server):
    """Test track_to_dict with missing optional fields."""
    mock_instance = MagicMock()
    mock_plex_server.return_value = mock_instance

    plex_music = plex_lib.PlexMusic('http://localhost:32400', 'token')

    # Mock Track object with missing fields
    mock_track = MagicMock()
    mock_track.title = "Song Title"
    mock_track.grandparentTitle = "Artist Name"
    mock_track.parentTitle = "Album Name"
    # Simulate missing parentYear
    del mock_track.parentYear
    mock_track.userRating = 5.0
    mock_track.viewCount = 10
    mock_track.lastRatedAt = None
    mock_track.media = []

    # Test
    track_dict = plex_music.track_to_dict(mock_track)

    self.assertEqual(track_dict['title'], "Song Title")
    self.assertEqual(track_dict['parentYear'], None)
    self.assertEqual(track_dict['lastRatedAt'], None)
    self.assertEqual(track_dict['Media'], [])

  @patch('plex_lib.PlexServer')
  def test_get_playlist_id_by_name(self, mock_plex_server):
    """Test get_playlist_id_by_name."""
    mock_instance = MagicMock()
    mock_plex_server.return_value = mock_instance

    plex_music = plex_lib.PlexMusic('http://localhost:32400', 'token')

    # Mock get_playlists
    mock_playlist1 = MagicMock()
    mock_playlist1.title = "Playlist 1"
    mock_playlist1.ratingKey = 123
    mock_playlist2 = MagicMock()
    mock_playlist2.title = "Playlist 2"
    mock_playlist2.ratingKey = 456

    plex_music.get_playlists = MagicMock(return_value=[mock_playlist1, mock_playlist2])

    # Test found
    self.assertEqual(plex_music.get_playlist_id_by_name("Playlist 1"), 123)
    # Test not found
    self.assertIsNone(plex_music.get_playlist_id_by_name("Nonexistent"))


if __name__ == "__main__":
  unittest.main()

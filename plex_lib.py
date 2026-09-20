from datetime import datetime
import sys
from typing import Dict, List, Optional, Union

from plexapi.audio import Track
from plexapi.playlist import Playlist
from plexapi.server import PlexServer
import requests


class _StubTrack:
    """Lightweight stub to mimic a Track object for playlist creation."""

    def __init__(self, server, ratingKey: str):
        self.ratingKey = ratingKey
        self.listType = 'audio'
        # plexapi requires _server to handle cross-server logic
        self._server = server


class PlexMusic:
    """A class to interact with the Plex Music library."""

    def __init__(self, server_url: str, token: str, library_name: str = 'Music'):
        """Initializes the PlexMusic object.

        Args:
            server_url (str): The URL of the Plex server.
            token (str): The Plex authentication token.
            library_name (str, optional): The name of the music library section.
                                         Defaults to 'Music'.
        """
        self.server = PlexServer(server_url, token)
        self.music = self.server.library.section(library_name)
        self._playlists: Optional[List[Playlist]] = None
        self._tracks: Optional[List[Dict]] = None

    def display_libraries(self):
        """Displays detailed information about each library section."""
        print("Plex Libraries:")
        for section in self.server.library.sections():
            print(f"  ID: {section.key}")
            print(f"  Type: {section.type}")
            print(f"  Title: {section.title}")
            print(f"  Agent: {section.agent}")
            print(f"  Scanner: {section.scanner}")
            print(f"  Location(s): {', '.join(section.locations)}")
            print(f"  Language: {section.language}")
            print("-" * 20)

    def time_ago_in_days(self, timestamp: int) -> int:
        """Returns the number of days ago from the given timestamp."""
        return (datetime.now() - datetime.fromtimestamp(timestamp)).days

    def get_playlists(self) -> List[Playlist]:
        """Fetches and caches the playlists from the Plex music library."""
        # Force refresh to ensure we have latest state for sync operations
        self._playlists = self.music.playlists()
        return self._playlists

    def get_playlist_id_by_name(self, playlist_name: str) -> Optional[int]:
        """Gets the playlist ID by its name from the cached playlists.

        Args:
            playlist_name (str): The name of the playlist.

        Returns:
            Optional[int]: The playlist ID (ratingKey) if found, None otherwise.
        """
        for playlist in self.get_playlists():
            if playlist.title == playlist_name:
                return playlist.ratingKey
        return None

    def get_playlist_tracks(self, playlist_id: Union[int, str]) -> List[Track]:
        """Fetches the tracks from a specified playlist by ID or title."""
        try:
            playlist = self.server.playlist(playlist_id)
            return playlist.items()
        except Exception:
            return []

    def cleanup_small_playlists(self, min_size: int, dry_run: bool = False) -> int:
        """
        Deletes music playlists containing fewer than `min_size` tracks.

        Uses `leafCount` to evaluate playlist sizes efficiently without fetching
        full track metadata.

        Args:
            min_size: Minimum track count required to keep a playlist.
            dry_run: If True, logs intended deletions without executing them.

        Returns:
            Count of playlists deleted (or marked for deletion).
        """
        deleted_count = 0
        # Refresh cache to get latest leafCount after sync
        playlists = self.get_playlists()

        for pl in playlists:
            if pl.leafCount < min_size:
                status = "[DRY RUN] Would delete" if dry_run else "[Cleanup] Deleting"
                print(
                    f"    {status} playlist '{pl.title}' (Tracks: {pl.leafCount})")

                if not dry_run:
                    try:
                        pl.delete()
                        deleted_count += 1
                    except Exception as e:
                        print(f"    [!] Failed to delete '{pl.title}': {e}")
        return deleted_count

    def get_tracks(
        self,
        limit: Optional[int] = None,
        batch_size: int = 5000,
        progress: bool = True,
    ) -> List[Dict]:
        """Fetches track metadata, paginating to avoid server timeouts.

        The full library (~95k tracks) is too large for Plex to serialize in a
        single response, so results are pulled in chunks. Only complete fetches
        are cached.

        Args:
            limit: Maximum number of tracks to fetch (None = all).
            batch_size: Page size used for each request.
            progress: Whether to print progress while paginating.

        Returns:
            List of track metadata dictionaries.
        """
        if self._tracks is not None:
            return self._tracks[:limit] if limit else self._tracks

        url = (
            f"{self.music._server._baseurl}/library/sections/"
            f"{self.music.key}/all"
        )
        headers = {'Accept': 'application/json'}
        page_size = min(batch_size, limit) if limit else batch_size

        tracks: List[Dict] = []
        total = None
        while True:
            params = {
                'type': '10',
                'X-Plex-Token': self.music._server._token,
                'X-Plex-Container-Start': len(tracks),
                'X-Plex-Container-Size': page_size,
            }
            response = requests.get(
                url, params=params, headers=headers, timeout=60
            )
            response.raise_for_status()
            container = response.json().get('MediaContainer', {})
            total = container.get('totalSize', 0) if total is None else total
            batch = container.get('Metadata', [])
            tracks.extend(batch)

            if progress and total:
                pct = min(100.0, len(tracks) / total * 100)
                print(
                    f'  [Plex] Fetched {len(tracks)}/{total} tracks '
                    f'({pct:.1f}%)',
                    flush=True,
                )
            if len(batch) < page_size or (limit and len(tracks) >= limit):
                break

        # Only cache a complete library fetch to avoid serving truncated data.
        if not limit:
            self._tracks = tracks
        return tracks[:limit] if limit else tracks

    def get_track(self, rating_key: str) -> Track:
        """Retrieves a plexapi.audio.Track object using its rating key.

        Args:
            rating_key (str): The rating key of the track.

        Returns:
            Track: The Track object if found, otherwise raises an exception.
        """
        return self.music.fetchItem(f'/library/metadata/{rating_key}')

    def get_recently_rated_tracks(self, limit: int = 100) -> List[Dict]:
        """Fetches the most recently rated tracks using a server-side sort.

        Sorting server-side avoids pulling the entire library just to find the
        newest ratings. Errors are intentionally not swallowed: these keys guard
        recent Plex ratings from being overwritten by MusicBee.

        Args:
            limit: Maximum number of tracks to return.

        Returns:
            List of recently rated track metadata dictionaries.
        """
        url = (
            f"{self.music._server._baseurl}/library/sections/"
            f"{self.music.key}/all"
        )
        params = {
            'type': '10',
            'sort': 'lastRatedAt:desc',
            'X-Plex-Token': self.music._server._token,
            'X-Plex-Container-Start': 0,
            'X-Plex-Container-Size': limit,
        }
        response = requests.get(
            url, params=params, headers={'Accept': 'application/json'},
            timeout=30
        )
        response.raise_for_status()
        tracks = response.json().get('MediaContainer', {}).get('Metadata', [])
        # Unrated tracks sort last; drop them so the guard list stays accurate.
        return [t for t in tracks if 'lastRatedAt' in t]

    def track_to_dict(self, track: Track) -> Dict:
        """Converts a plexapi Track object to a dictionary."""
        return {
            'title': track.title,
            'grandparentTitle': track.grandparentTitle,
            'parentTitle': track.parentTitle,
            'parentYear': getattr(track, 'parentYear', None),
            'userRating': track.userRating,
            'viewCount': track.viewCount,
            'lastRatedAt': track.lastRatedAt.timestamp() if track.lastRatedAt else None,
            'Media': [{'audioCodec': media_item.audioCodec} for media_item in track.media]
        }

    def display_tracks(self, tracks: List[Union[Track, Dict]], show_details: bool = False):
        """Displays information about tracks (dictionaries or Track objects)."""
        for i, track in enumerate(tracks, start=1):
            if isinstance(track, Track):
                track = self.track_to_dict(track)

            title = track.get('title', '')
            artist = track.get('grandparentTitle', '')
            album = track.get('parentTitle', '')
            year = track.get('parentYear', '')
            track_info = f"{i+1}. '{title}' by {artist} "
            if album:
                track_info += f"from {album} "
            if year:
                track_info += f"({year})"

            if show_details and track.get('lastRatedAt'):
                rating = track.get('userRating')
                view_count = track.get('viewCount')
                codec = track.get('Media', [{}])[0].get(
                    'audioCodec', 'Unknown').upper()

                track_info += (
                    f" [Rated {rating/2:.1f} stars on "
                    f"{datetime.fromtimestamp(track['lastRatedAt']).strftime('%Y-%m-%d')}"
                )
                if view_count:
                    track_info += f", {view_count} plays"
                track_info += f", {codec}]"

            try:
                print(track_info)
            except UnicodeEncodeError:
                print(track_info.encode(sys.stdout.encoding,
                      errors='replace').decode(sys.stdout.encoding))

    def display_playlists(self):
        """Displays information about cached playlists using plexapi."""
        for i, playlist in enumerate(self.get_playlists(), start=1):
            print(
                f"Playlist {i}: '{playlist.title}', Entries: {playlist.leafCount}, "
                f"Updated: {self.time_ago_in_days(playlist.updatedAt.timestamp())} days ago"
            )

    def sync_playlist(self, title: str, rating_keys: List[str], existing_playlist: Optional[Playlist] = None) -> bool:
        """Creates or replaces a playlist with the given rating keys.
        Handles batching to prevent HTTP 400 errors for large playlists.

        Args:
            title (str): The name of the playlist.
            rating_keys (List[str]): List of ratingKey IDs to add.
            existing_playlist (Optional[Playlist]): The existing Plex playlist object to replace.

        Returns:
            bool: True if created, False if rating_keys is empty or error.
        """
        if not rating_keys:
            return False

        # 1. Delete existing if passed
        if existing_playlist:
            try:
                existing_playlist.delete()
            except Exception as e:
                print(f"    [!] Error deleting old playlist '{title}': {e}")

        # 2. Create new with Batching
        # Batch size 1000 maximizes HTTP throughput while remaining safe for local Plex API
        BATCH_SIZE = 1000



        initial_keys = rating_keys[:BATCH_SIZE]
        remaining_keys = rating_keys[BATCH_SIZE:]

        # Create Stubs with self.server passed in
        initial_items = [_StubTrack(self.server, k) for k in initial_keys]

        try:
            # Create with first batch
            playlist = Playlist.create(self.server, title, items=initial_items)

            # Append remaining batches
            if remaining_keys:
                for i in range(0, len(remaining_keys), BATCH_SIZE):
                    chunk_keys = remaining_keys[i: i + BATCH_SIZE]
                    # Pass self.server to stubs here too
                    chunk_items = [_StubTrack(self.server, k)
                                   for k in chunk_keys]
                    try:
                        playlist.addItems(chunk_items)
                    except Exception as e:
                        print(
                            f"    [!] Error appending items to '{title}' (batch {i}): {e}")

            return True
        except Exception as e:
            print(f"    [!] Error creating playlist '{title}': {e}")
            return False

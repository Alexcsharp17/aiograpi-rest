import json
import types
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

import aiograpi_rest.helpers as helpers
import aiograpi_rest.routers.clip as clip_router
import aiograpi_rest.routers.igtv as igtv_router
import aiograpi_rest.routers.photo as photo_router
import aiograpi_rest.routers.story as story_router
import aiograpi_rest.routers.video as video_router
from aiograpi_rest.dependencies import get_clients
from aiograpi_rest.main import app


def _user_short():
    return {"pk": "42", "username": "u", "full_name": "Full"}


def _media_payload():
    return {
        "pk": 1,
        "id": "1_42",
        "code": "abc",
        "taken_at": "2026-01-01T00:00:00+00:00",
        "media_type": 1,
        "user": _user_short(),
        "like_count": 0,
        "caption_text": "",
        "usertags": [],
        "sponsor_tags": [],
    }


def _story_payload():
    return {
        "pk": 1,
        "id": "1_42",
        "code": "abc",
        "taken_at": "2026-01-01T00:00:00+00:00",
        "media_type": 1,
        "user": _user_short(),
        "sponsor_tags": [],
        "mentions": [],
        "links": [],
        "hashtags": [],
        "locations": [],
        "stickers": [],
    }


def _track_payload():
    return {
        "id": "track1",
        "title": "Track",
        "subtitle": "Artist",
        "display_artist": "Artist",
        "audio_cluster_id": 1,
        "highlight_start_times_in_ms": [1000],
        "is_explicit": False,
        "dash_manifest": "",
        "uri": "https://example.test/track.m4a",
        "has_lyrics": False,
        "audio_asset_id": 2,
        "duration_in_ms": 30000,
        "allows_saving": True,
        "territory_validity_periods": {},
    }


class FakeClient:
    def __init__(self):
        self.calls = []

    # Downloads
    async def photo_download(self, media_pk, folder=""):
        self.calls.append(("photo_download", media_pk, str(folder)))
        return Path(__file__).resolve()

    async def photo_download_by_url(self, url, filename, folder):
        self.calls.append(("photo_download_by_url", url, filename, str(folder)))
        return Path(__file__).resolve()

    async def video_download(self, media_pk, folder=""):
        self.calls.append(("video_download", media_pk, str(folder)))
        return Path(__file__).resolve()

    async def video_download_by_url(self, url, filename, folder):
        self.calls.append(("video_download_by_url", url, filename, str(folder)))
        return Path(__file__).resolve()

    async def clip_download(self, media_pk, folder=""):
        self.calls.append(("clip_download", media_pk, str(folder)))
        return Path(__file__).resolve()

    async def clip_download_by_url(self, url, filename, folder):
        self.calls.append(("clip_download_by_url", url, filename, str(folder)))
        return Path(__file__).resolve()

    async def media_template_v1(self, media_id):
        self.calls.append(("media_template_v1", media_id))
        return {"template_clips_media_id": media_id, "status": "ok"}

    async def clip_info_for_creation(self):
        self.calls.append(("clip_info_for_creation",))
        return {"trial_config": {"is_enabled": True}}

    async def clip_trial_eligible(self):
        self.calls.append(("clip_trial_eligible",))
        return True

    async def clip_share_to_fb_config(self, device_status=None):
        self.calls.append(("clip_share_to_fb_config", device_status))
        return {"device_status": device_status, "status": "ok"}

    async def clip_pin(self, media_pk, revert=False):
        self.calls.append(("clip_pin", media_pk, revert))
        return True

    async def clip_unpin(self, media_pk):
        self.calls.append(("clip_unpin", media_pk))
        return True

    async def igtv_download(self, media_pk, folder=""):
        self.calls.append(("igtv_download", media_pk, str(folder)))
        return Path(__file__).resolve()

    async def igtv_download_by_url(self, url, filename, folder):
        self.calls.append(("igtv_download_by_url", url, filename, str(folder)))
        return Path(__file__).resolve()

    async def album_download(self, media_pk, folder=""):
        self.calls.append(("album_download", media_pk, str(folder)))
        return [Path(__file__).resolve(), Path(__file__).resolve()]

    async def album_download_by_urls(self, urls, folder):
        self.calls.append(("album_download_by_urls", tuple(urls), str(folder)))
        return [Path(__file__).resolve()]

    # Uploads (called from helpers)
    async def photo_upload(self, path, **kwargs):
        self.calls.append(("photo_upload", path, kwargs))
        return _media_payload()

    async def photo_upload_with_music(self, path, **kwargs):
        upload_path = Path(path)
        self.calls.append(
            (
                "photo_upload_with_music",
                upload_path.suffix,
                upload_path.read_bytes(),
                kwargs["caption"],
                kwargs["track"].id,
                kwargs["upload_id"],
                kwargs["extra_data"],
                kwargs["audio_asset_start_time"],
                kwargs["overlap_duration"],
                kwargs["browse_session_id"],
                kwargs["alacorn_session_id"],
            )
        )
        return _media_payload()

    async def video_upload(self, path, **kwargs):
        self.calls.append(("video_upload", path, kwargs))
        return _media_payload()

    async def album_upload(self, paths, **kwargs):
        self.calls.append(("album_upload", tuple(paths), kwargs))
        return _media_payload()

    async def album_upload_with_music(self, paths, **kwargs):
        upload_paths = [Path(path) for path in paths]
        self.calls.append(
            (
                "album_upload_with_music",
                tuple(path.suffix for path in upload_paths),
                tuple(path.read_bytes() for path in upload_paths),
                kwargs["caption"],
                kwargs["track"].id,
                kwargs["configure_timeout"],
                kwargs["extra_data"],
                kwargs["audio_asset_start_time"],
                kwargs["overlap_duration"],
                kwargs["browse_session_id"],
                kwargs["alacorn_session_id"],
            )
        )
        return _media_payload()

    async def igtv_upload(self, path, **kwargs):
        self.calls.append(("igtv_upload", path, kwargs))
        return _media_payload()

    async def clip_upload(self, path, **kwargs):
        self.calls.append(("clip_upload", path, kwargs))
        return _media_payload()

    async def clip_upload_with_music(self, path, caption, track, **kwargs):
        upload_path = Path(path)
        thumbnail = kwargs.get("thumbnail")
        thumbnail_path = Path(thumbnail) if thumbnail else None
        self.calls.append(
            (
                "clip_upload_with_music",
                upload_path.suffix,
                upload_path.read_bytes(),
                caption,
                track.id,
                thumbnail_path.suffix if thumbnail_path else None,
                thumbnail_path.read_bytes() if thumbnail_path else None,
                kwargs["usertags"],
                kwargs["location"],
                kwargs["extra_data"],
                kwargs["audio_asset_start_time"],
                kwargs["overlap_duration"],
                kwargs["original_volume"],
                kwargs["music_volume"],
                kwargs["product"],
                kwargs["alacorn_session_id"],
            )
        )
        return _media_payload()

    async def photo_upload_to_story(self, path, **kwargs):
        self.calls.append(("photo_upload_to_story", path, kwargs))
        return _story_payload()

    async def video_upload_to_story(self, path, **kwargs):
        self.calls.append(("video_upload_to_story", path, kwargs))
        return _story_payload()


class FakeStorage:
    def __init__(self):
        self.client = FakeClient()

    async def get(self, sessionid):
        return self.client

    def close(self):
        pass


@pytest.fixture
def storage():
    fake = FakeStorage()
    app.dependency_overrides[get_clients] = lambda: fake
    yield fake
    app.dependency_overrides.clear()


@pytest.fixture
def fake_requests(monkeypatch):
    """Replace requests.get with a fake that returns a small byte payload."""
    class FakeResponse:
        def __init__(self, content):
            self.content = content

    def fake_get(url, *args, **kwargs):
        return FakeResponse(b"fake-bytes")

    monkeypatch.setattr(photo_router, "requests", types.SimpleNamespace(get=fake_get))
    monkeypatch.setattr(video_router, "requests", types.SimpleNamespace(get=fake_get))
    monkeypatch.setattr(clip_router, "requests", types.SimpleNamespace(get=fake_get))
    monkeypatch.setattr(igtv_router, "requests", types.SimpleNamespace(get=fake_get))
    monkeypatch.setattr(story_router, "requests", types.SimpleNamespace(get=fake_get))


@pytest.fixture
def fake_storybuilder(monkeypatch):
    class FakeVideo:
        def __init__(self, path):
            self.path = path

    class FakeStoryBuilder:
        def __init__(self, path, caption, mentions, bgpath=None):
            self.path = path
            self.caption = caption
            self.mentions = mentions

        def photo(self, duration):
            return FakeVideo(self.path)

        def video(self, duration):
            return FakeVideo(self.path)

    monkeypatch.setattr(helpers, "StoryBuilder", FakeStoryBuilder)


# Photo routes
@pytest.mark.asyncio
async def test_photo_download_returns_path_when_returnfile_false(storage):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get(
            "/photo/download",
            params={"sessionid": "sid", "media_pk": "1", "returnFile": "false"},
        )
    assert response.status_code == 200
    assert response.json().endswith("test_upload_download_routes.py")


@pytest.mark.asyncio
async def test_photo_download_returns_file_when_returnfile_true(storage):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get(
            "/photo/download",
            params={"sessionid": "sid", "media_pk": "1"},
        )
    assert response.status_code == 200
    assert b"import pytest" in response.content


@pytest.mark.asyncio
async def test_photo_download_by_url_returns_path_when_returnfile_false(storage):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get(
            "/photo/download/by/url",
            params={
                "sessionid": "sid",
                "url": "https://x/y.jpg",
                "returnFile": "false",
            },
        )
    assert response.status_code == 200
    assert response.json().endswith("test_upload_download_routes.py")


@pytest.mark.asyncio
async def test_photo_download_by_url_returns_file_when_returnfile_true(storage):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get(
            "/photo/download/by/url",
            params={"sessionid": "sid", "url": "https://x/y.jpg"},
        )
    assert response.status_code == 200
    assert b"import pytest" in response.content


@pytest.mark.asyncio
async def test_photo_upload_uses_helper(storage):
    usertag = json.dumps({"user": {"pk": 1, "username": "u", "full_name": "f"}, "x": 0.5, "y": 0.5})
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.post(
            "/photo/upload",
            data={"sessionid": "sid", "caption": "hi", "usertags": usertag},
            files={"file": ("a.jpg", b"img-bytes", "image/jpeg")},
        )
    assert response.status_code == 200
    assert any(call[0] == "photo_upload" for call in storage.client.calls)


@pytest.mark.asyncio
async def test_photo_upload_with_music_uses_track_and_extra_data(storage):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.post(
            "/photo/upload/with/music",
            data={
                "sessionid": "sid",
                "caption": "hi",
                "track": json.dumps(_track_payload()),
                "upload_id": "upload1",
                "extra_data": json.dumps({"share_to_facebook": "1"}),
                "audio_asset_start_time": "1000",
                "overlap_duration": "25000",
                "browse_session_id": "browse1",
                "alacorn_session_id": "alacorn1",
            },
            files={"file": ("a.jpg", b"photo-music-bytes", "image/jpeg")},
        )

    assert response.status_code == 200
    assert (
        "photo_upload_with_music",
        ".jpg",
        b"photo-music-bytes",
        "hi",
        "track1",
        "upload1",
        {"share_to_facebook": "1"},
        1000,
        25000,
        "browse1",
        "alacorn1",
    ) in storage.client.calls


@pytest.mark.asyncio
async def test_photo_upload_ignores_blank_optional_usertags_and_location(storage):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.post(
            "/photo/upload",
            data={"sessionid": "sid", "caption": "hi", "usertags": "", "location": ""},
            files={"file": ("a.jpg", b"img-bytes", "image/jpeg")},
        )
    assert response.status_code == 200
    call = next(call for call in storage.client.calls if call[0] == "photo_upload")
    assert call[2]["usertags"] == []
    assert call[2]["location"] is None


@pytest.mark.asyncio
async def test_photo_upload_accepts_json_array_usertags_and_json_location(storage):
    usertags = json.dumps(
        [
            {"user": {"pk": 1, "username": "u", "full_name": "f"}, "x": 0.5, "y": 0.5},
            {"user": {"pk": 2, "username": "v", "full_name": "g"}, "x": 0.2, "y": 0.8},
        ]
    )
    location = json.dumps({"pk": 1, "name": "Place", "lat": 10.0, "lng": 20.0})
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.post(
            "/photo/upload",
            data={"sessionid": "sid", "caption": "hi", "usertags": usertags, "location": location},
            files={"file": ("a.jpg", b"img-bytes", "image/jpeg")},
        )
    assert response.status_code == 200
    call = next(call for call in storage.client.calls if call[0] == "photo_upload")
    assert [tag.user.pk for tag in call[2]["usertags"]] == ["1", "2"]
    assert call[2]["location"].name == "Place"


@pytest.mark.asyncio
async def test_photo_upload_rejects_invalid_usertags_json(storage):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.post(
            "/photo/upload",
            data={"sessionid": "sid", "caption": "hi", "usertags": "{bad-json"},
            files={"file": ("a.jpg", b"img-bytes", "image/jpeg")},
        )
    assert response.status_code == 422
    assert response.json()["detail"] == "Invalid JSON object for form field 'usertags'"


@pytest.mark.asyncio
async def test_photo_upload_rejects_invalid_location_json(storage):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.post(
            "/photo/upload",
            data={"sessionid": "sid", "caption": "hi", "location": "{bad-json"},
            files={"file": ("a.jpg", b"img-bytes", "image/jpeg")},
        )
    assert response.status_code == 422
    assert response.json()["detail"] == "Invalid JSON object for form field 'location'"


@pytest.mark.asyncio
async def test_photo_upload_by_url_uses_helper(storage, fake_requests):
    usertag = json.dumps({"user": {"pk": 1, "username": "u", "full_name": "f"}, "x": 0.5, "y": 0.5})
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.post(
            "/photo/upload/by/url",
            data={
                "sessionid": "sid",
                "url": "https://example.com/photo.jpg",
                "caption": "hello",
                "usertags": usertag,
            },
        )
    assert response.status_code == 200
    assert any(call[0] == "photo_upload" for call in storage.client.calls)


@pytest.mark.asyncio
async def test_photo_upload_to_story_as_photo(storage):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.post(
            "/story/upload",
            data={"sessionid": "sid", "caption": "hi", "as_video": "false"},
            files={"file": ("a.jpg", b"img-bytes", "image/jpeg")},
        )
    assert response.status_code == 200
    assert any(call[0] == "photo_upload_to_story" for call in storage.client.calls)


@pytest.mark.asyncio
async def test_photo_upload_to_story_as_video(storage, fake_storybuilder):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.post(
            "/story/upload",
            data={"sessionid": "sid", "caption": "hi", "as_video": "true"},
            files={"file": ("a.jpg", b"img-bytes", "image/jpeg")},
        )
    assert response.status_code == 200
    assert any(call[0] == "video_upload_to_story" for call in storage.client.calls)


@pytest.mark.asyncio
async def test_photo_upload_to_story_by_url_as_photo(storage, fake_requests):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.post(
            "/story/upload/by/url",
            data={
                "sessionid": "sid",
                "url": "https://example.com/photo.jpg",
                "as_video": "false",
            },
        )
    assert response.status_code == 200
    assert any(call[0] == "photo_upload_to_story" for call in storage.client.calls)


@pytest.mark.asyncio
async def test_photo_upload_to_story_by_url_as_video(storage, fake_requests, fake_storybuilder):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.post(
            "/story/upload/by/url",
            data={
                "sessionid": "sid",
                "url": "https://example.com/photo.jpg",
                "as_video": "true",
            },
        )
    assert response.status_code == 200
    assert any(call[0] == "video_upload_to_story" for call in storage.client.calls)


@pytest.mark.asyncio
async def test_story_upload_by_url_accepts_json_mentions(storage, fake_requests):
    mention = json.dumps([{"user": {"pk": "42", "username": "u", "full_name": "Full"}}])
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.post(
            "/story/upload/by/url",
            data={
                "sessionid": "sid",
                "url": "https://example.com/photo.jpg",
                "mentions": mention,
            },
        )
    assert response.status_code == 200
    story_call = next(call for call in storage.client.calls if call[0] == "photo_upload_to_story")
    mention_model = story_call[2]["mentions"][0]
    assert mention_model.user.pk == "42"


@pytest.mark.asyncio
async def test_story_upload_defaults_missing_mention_geometry(storage):
    mention = json.dumps({"user": {"pk": "42", "username": "u", "full_name": "Full"}})
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.post(
            "/story/upload",
            data={"sessionid": "sid", "mentions": mention},
            files={"file": ("a.jpg", b"img-bytes", "image/jpeg")},
        )
    assert response.status_code == 200
    story_call = next(call for call in storage.client.calls if call[0] == "photo_upload_to_story")
    mention_model = story_call[2]["mentions"][0]
    assert mention_model.x == 0.5
    assert mention_model.y == 0.5
    assert mention_model.width == 0.5
    assert mention_model.height == 0.2
    assert mention_model.rotation == 0.0


@pytest.mark.asyncio
async def test_story_upload_keeps_explicit_mention_geometry(storage):
    mention = json.dumps(
        {
            "user": {"pk": "42", "username": "u", "full_name": "Full"},
            "x": 0.1,
            "y": 0.2,
            "width": 0.3,
            "height": 0.4,
            "rotation": 12.0,
        }
    )
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.post(
            "/story/upload",
            data={"sessionid": "sid", "mentions": mention},
            files={"file": ("a.jpg", b"img-bytes", "image/jpeg")},
        )
    assert response.status_code == 200
    story_call = next(call for call in storage.client.calls if call[0] == "photo_upload_to_story")
    mention_model = story_call[2]["mentions"][0]
    assert mention_model.x == 0.1
    assert mention_model.y == 0.2
    assert mention_model.width == 0.3
    assert mention_model.height == 0.4
    assert mention_model.rotation == 12.0


@pytest.mark.asyncio
async def test_story_upload_by_url_rejects_invalid_json_mentions(storage, fake_requests):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.post(
            "/story/upload/by/url",
            data={
                "sessionid": "sid",
                "url": "https://example.com/photo.jpg",
                "mentions": "{bad-json",
            },
        )
    assert response.status_code == 422
    assert response.json()["detail"] == "Invalid JSON object for form field 'mentions'"


# Video routes
@pytest.mark.asyncio
async def test_video_download_returns_path_when_returnfile_false(storage):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get(
            "/video/download",
            params={"sessionid": "sid", "media_pk": "1", "returnFile": "false"},
        )
    assert response.status_code == 200
    assert response.json().endswith("test_upload_download_routes.py")


@pytest.mark.asyncio
async def test_video_download_returns_file_when_returnfile_true(storage):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get(
            "/video/download",
            params={"sessionid": "sid", "media_pk": "1"},
        )
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_video_download_by_url_returns_path_when_returnfile_false(storage):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get(
            "/video/download/by/url",
            params={
                "sessionid": "sid",
                "url": "https://x/y.mp4",
                "returnFile": "false",
            },
        )
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_video_download_by_url_returns_file_when_returnfile_true(storage):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get(
            "/video/download/by/url",
            params={"sessionid": "sid", "url": "https://x/y.mp4"},
        )
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_video_upload_with_and_without_thumbnail(storage):
    usertag = json.dumps({"user": {"pk": 1, "username": "u", "full_name": "f"}, "x": 0.5, "y": 0.5})
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        no_thumb = await ac.post(
            "/video/upload",
            data={"sessionid": "sid", "caption": "hi", "usertags": usertag},
            files={"file": ("a.mp4", b"vid-bytes", "video/mp4")},
        )
        with_thumb = await ac.post(
            "/video/upload",
            data={"sessionid": "sid", "caption": "hi"},
            files=[
                ("file", ("a.mp4", b"vid-bytes", "video/mp4")),
                ("thumbnail", ("t.jpg", b"thumb-bytes", "image/jpeg")),
            ],
        )
    assert no_thumb.status_code == 200
    assert with_thumb.status_code == 200
    upload_calls = [c for c in storage.client.calls if c[0] == "video_upload"]
    assert len(upload_calls) == 2


@pytest.mark.asyncio
async def test_video_upload_by_url_with_and_without_thumbnail(storage, fake_requests):
    usertag = json.dumps({"user": {"pk": 1, "username": "u", "full_name": "f"}, "x": 0.5, "y": 0.5})
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        no_thumb = await ac.post(
            "/video/upload/by/url",
            data={
                "sessionid": "sid",
                "url": "https://example.com/v.mp4",
                "caption": "hi",
                "usertags": usertag,
            },
        )
        with_thumb = await ac.post(
            "/video/upload/by/url",
            data={
                "sessionid": "sid",
                "url": "https://example.com/v.mp4",
                "caption": "hi",
            },
            files=[("thumbnail", ("t.jpg", b"thumb-bytes", "image/jpeg"))],
        )
    assert no_thumb.status_code == 200
    assert with_thumb.status_code == 200


@pytest.mark.asyncio
async def test_story_upload_video(storage, fake_storybuilder):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.post(
            "/story/upload",
            data={"sessionid": "sid", "caption": "hi"},
            files={"file": ("a.mp4", b"vid-bytes", "video/mp4")},
        )
    assert response.status_code == 200
    assert any(call[0] == "video_upload_to_story" for call in storage.client.calls)


@pytest.mark.asyncio
async def test_story_upload_video_by_url(storage, fake_requests, fake_storybuilder):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.post(
            "/story/upload/by/url",
            data={
                "sessionid": "sid",
                "url": "https://example.com/v.mp4",
                "caption": "hi",
            },
        )
    assert response.status_code == 200


# Clip routes
@pytest.mark.asyncio
async def test_clip_download_routes_both_returnfile_modes(storage):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        path_resp = await ac.get(
            "/clip/download",
            params={"sessionid": "sid", "media_pk": "1", "returnFile": "false"},
        )
        file_resp = await ac.get(
            "/clip/download",
            params={"sessionid": "sid", "media_pk": "1"},
        )
    assert path_resp.status_code == 200
    assert file_resp.status_code == 200
    assert path_resp.json().endswith("test_upload_download_routes.py")


@pytest.mark.asyncio
async def test_clip_download_by_url_both_returnfile_modes(storage):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        path_resp = await ac.get(
            "/clip/download/by/url",
            params={
                "sessionid": "sid",
                "url": "https://x/y.mp4",
                "returnFile": "false",
            },
        )
        file_resp = await ac.get(
            "/clip/download/by/url",
            params={"sessionid": "sid", "url": "https://x/y.mp4"},
        )
    assert path_resp.status_code == 200
    assert file_resp.status_code == 200


@pytest.mark.asyncio
async def test_clip_template_returns_raw_template(storage):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/clip/template", params={"sessionid": "sid", "media_id": "clip1"})

    assert response.status_code == 200
    assert response.json()["template_clips_media_id"] == "clip1"
    assert ("media_template_v1", "clip1") in storage.client.calls


@pytest.mark.asyncio
async def test_clip_creation_preflight_routes(storage):
    device_status = {"hw_av1_dec": True, "chip_name": "m1"}
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        info = await ac.get("/clip/creation/info", params={"sessionid": "sid"})
        trial = await ac.get("/clip/trial-eligibility", params={"sessionid": "sid"})
        fb_default = await ac.get("/clip/share/facebook/config", params={"sessionid": "sid"})
        fb_custom = await ac.get(
            "/clip/share/facebook/config",
            params={"sessionid": "sid", "device_status": json.dumps(device_status)},
        )

    assert info.status_code == 200
    assert info.json()["trial_config"]["is_enabled"] is True
    assert trial.status_code == 200
    assert trial.json() is True
    assert fb_default.status_code == 200
    assert fb_default.json()["device_status"] is None
    assert fb_custom.status_code == 200
    assert fb_custom.json()["device_status"] == device_status
    assert ("clip_info_for_creation",) in storage.client.calls
    assert ("clip_trial_eligible",) in storage.client.calls
    assert ("clip_share_to_fb_config", None) in storage.client.calls
    assert ("clip_share_to_fb_config", device_status) in storage.client.calls


@pytest.mark.asyncio
async def test_clip_pin_and_upload_with_music(storage):
    track = {
        "id": "track1",
        "title": "Track",
        "subtitle": "Artist",
        "display_artist": "Artist",
        "audio_cluster_id": 1,
        "highlight_start_times_in_ms": [1000],
        "is_explicit": False,
        "dash_manifest": "",
        "uri": "https://example.test/track.m4a",
        "has_lyrics": False,
        "audio_asset_id": 2,
        "duration_in_ms": 30000,
        "allows_saving": True,
        "territory_validity_periods": {},
    }
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        pin = await ac.post("/clip/pin", data={"sessionid": "sid", "media_pk": "1"})
        unpin = await ac.delete("/clip/pin", params={"sessionid": "sid", "media_pk": "1"})
        upload = await ac.post(
            "/clip/upload/with/music",
            data={
                "sessionid": "sid",
                "caption": "music",
                "track": json.dumps(track),
                "extra_data": json.dumps({"share_to_facebook": "1"}),
                "audio_asset_start_time": "1000",
                "overlap_duration": "25000",
                "original_volume": "0.75",
                "music_volume": "0.85",
                "product": "clips",
                "alacorn_session_id": "alacorn-1",
            },
            files={
                "file": ("a.mp4", b"clip-music-bytes", "video/mp4"),
                "thumbnail": ("cover.jpg", b"cover-bytes", "image/jpeg"),
            },
        )

    assert pin.status_code == 200
    assert pin.json() is True
    assert unpin.status_code == 200
    assert unpin.json() is True
    assert upload.status_code == 200
    assert ("clip_pin", "1", False) in storage.client.calls
    assert ("clip_unpin", "1") in storage.client.calls
    upload_call = next(call for call in storage.client.calls if call[0] == "clip_upload_with_music")
    assert upload_call[:7] == (
        "clip_upload_with_music",
        ".mp4",
        b"clip-music-bytes",
        "music",
        "track1",
        ".jpg",
        b"cover-bytes",
    )
    assert upload_call[7] == []
    assert upload_call[8] is None
    assert upload_call[9] == {"share_to_facebook": "1"}
    assert upload_call[10:] == (1000, 25000, 0.75, 0.85, "clips", "alacorn-1")


@pytest.mark.asyncio
async def test_clip_upload_with_and_without_thumbnail(storage):
    usertag = json.dumps({"user": {"pk": 1, "username": "u", "full_name": "f"}, "x": 0.5, "y": 0.5})
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        no_thumb = await ac.post(
            "/clip/upload",
            data={"sessionid": "sid", "caption": "hi", "usertags": usertag},
            files={"file": ("a.mp4", b"clip-bytes", "video/mp4")},
        )
        with_thumb = await ac.post(
            "/clip/upload",
            data={"sessionid": "sid", "caption": "hi"},
            files=[
                ("file", ("a.mp4", b"clip-bytes", "video/mp4")),
                ("thumbnail", ("t.jpg", b"thumb-bytes", "image/jpeg")),
            ],
        )
    assert no_thumb.status_code == 200
    assert with_thumb.status_code == 200
    upload_calls = [c for c in storage.client.calls if c[0] == "clip_upload"]
    assert len(upload_calls) == 2


@pytest.mark.asyncio
async def test_clip_upload_by_url_with_and_without_thumbnail(storage, fake_requests):
    usertag = json.dumps({"user": {"pk": 1, "username": "u", "full_name": "f"}, "x": 0.5, "y": 0.5})
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        no_thumb = await ac.post(
            "/clip/upload/by/url",
            data={
                "sessionid": "sid",
                "url": "https://example.com/c.mp4",
                "caption": "hi",
                "usertags": usertag,
            },
        )
        with_thumb = await ac.post(
            "/clip/upload/by/url",
            data={
                "sessionid": "sid",
                "url": "https://example.com/c.mp4",
                "caption": "hi",
            },
            files=[("thumbnail", ("t.jpg", b"thumb-bytes", "image/jpeg"))],
        )
    assert no_thumb.status_code == 200
    assert with_thumb.status_code == 200


# IGTV routes
@pytest.mark.asyncio
async def test_igtv_download_routes_both_returnfile_modes(storage):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        path_resp = await ac.get(
            "/igtv/download",
            params={"sessionid": "sid", "media_pk": "1", "returnFile": "false"},
        )
        file_resp = await ac.get(
            "/igtv/download",
            params={"sessionid": "sid", "media_pk": "1"},
        )
    assert path_resp.status_code == 200
    assert file_resp.status_code == 200


@pytest.mark.asyncio
async def test_igtv_download_by_url_both_returnfile_modes(storage):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        path_resp = await ac.get(
            "/igtv/download/by/url",
            params={
                "sessionid": "sid",
                "url": "https://x/y.mp4",
                "returnFile": "false",
            },
        )
        file_resp = await ac.get(
            "/igtv/download/by/url",
            params={"sessionid": "sid", "url": "https://x/y.mp4"},
        )
    assert path_resp.status_code == 200
    assert file_resp.status_code == 200


@pytest.mark.asyncio
async def test_igtv_upload_with_and_without_thumbnail(storage):
    usertag = json.dumps({"user": {"pk": 1, "username": "u", "full_name": "f"}, "x": 0.5, "y": 0.5})
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        no_thumb = await ac.post(
            "/igtv/upload",
            data={"sessionid": "sid", "title": "t", "caption": "hi", "usertags": usertag},
            files={"file": ("a.mp4", b"igtv-bytes", "video/mp4")},
        )
        with_thumb = await ac.post(
            "/igtv/upload",
            data={"sessionid": "sid", "title": "t", "caption": "hi"},
            files=[
                ("file", ("a.mp4", b"igtv-bytes", "video/mp4")),
                ("thumbnail", ("t.jpg", b"thumb-bytes", "image/jpeg")),
            ],
        )
    assert no_thumb.status_code == 200
    assert with_thumb.status_code == 200


@pytest.mark.asyncio
async def test_igtv_upload_by_url_with_and_without_thumbnail(storage, fake_requests):
    usertag = json.dumps({"user": {"pk": 1, "username": "u", "full_name": "f"}, "x": 0.5, "y": 0.5})
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        no_thumb = await ac.post(
            "/igtv/upload/by/url",
            data={
                "sessionid": "sid",
                "url": "https://example.com/i.mp4",
                "title": "t",
                "caption": "hi",
                "usertags": usertag,
            },
        )
        with_thumb = await ac.post(
            "/igtv/upload/by/url",
            data={
                "sessionid": "sid",
                "url": "https://example.com/i.mp4",
                "title": "t",
                "caption": "hi",
            },
            files=[("thumbnail", ("t.jpg", b"thumb-bytes", "image/jpeg"))],
        )
    assert no_thumb.status_code == 200
    assert with_thumb.status_code == 200


@pytest.mark.asyncio
async def test_album_upload_with_music_uses_track_and_extra_data(storage):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.post(
            "/album/upload/with/music",
            data={
                "sessionid": "sid",
                "caption": "hi",
                "track": json.dumps(_track_payload()),
                "configure_timeout": "5",
                "extra_data": json.dumps({"share_to_facebook": "1"}),
                "audio_asset_start_time": "1000",
                "overlap_duration": "25000",
                "browse_session_id": "browse1",
                "alacorn_session_id": "alacorn1",
            },
            files=[
                ("files", ("a.jpg", b"album-photo-1", "image/jpeg")),
                ("files", ("b.jpg", b"album-photo-2", "image/jpeg")),
            ],
        )

    assert response.status_code == 200
    assert (
        "album_upload_with_music",
        (".jpg", ".jpg"),
        (b"album-photo-1", b"album-photo-2"),
        "hi",
        "track1",
        5,
        {"share_to_facebook": "1"},
        1000,
        25000,
        "browse1",
        "alacorn1",
    ) in storage.client.calls


# Album routes
@pytest.mark.asyncio
async def test_album_download_returns_list(storage):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get(
            "/album/download",
            params={"sessionid": "sid", "media_pk": "1"},
        )
    assert response.status_code == 200
    assert isinstance(response.json(), list)
    assert len(response.json()) == 2


@pytest.mark.asyncio
async def test_album_download_by_urls_returns_list(storage):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get(
            "/album/download/by/urls",
            params={"sessionid": "sid", "urls": ["https://x/1.jpg", "https://x/2.jpg"]},
        )
    assert response.status_code == 200
    assert len(response.json()) == 1


@pytest.mark.asyncio
async def test_album_upload(storage):
    usertag = json.dumps({"user": {"pk": 1, "username": "u", "full_name": "f"}, "x": 0.5, "y": 0.5})
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.post(
            "/album/upload",
            data={"sessionid": "sid", "caption": "hi", "usertags": usertag},
            files=[
                ("files", ("a.jpg", b"img-1", "image/jpeg")),
                ("files", ("b.jpg", b"img-2", "image/jpeg")),
            ],
        )
    assert response.status_code == 200
    assert any(call[0] == "album_upload" for call in storage.client.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("route", "query", "method", "provider_args", "returns_list"),
    [
        ("/photo/download", {"media_pk": "17"}, "photo_download", (17,), False),
        ("/photo/download/by/url", {"url": "https://example.test/media"}, "photo_download_by_url", ("https://example.test/media", ""), False),
        ("/video/download", {"media_pk": "17"}, "video_download", (17,), False),
        ("/video/download/by/url", {"url": "https://example.test/media"}, "video_download_by_url", ("https://example.test/media", ""), False),
        ("/clip/download", {"media_pk": "17"}, "clip_download", (17,), False),
        ("/clip/download/by/url", {"url": "https://example.test/media"}, "clip_download_by_url", ("https://example.test/media", ""), False),
        ("/igtv/download", {"media_pk": "17"}, "igtv_download", (17,), False),
        ("/igtv/download/by/url", {"url": "https://example.test/media"}, "igtv_download_by_url", ("https://example.test/media", ""), False),
        ("/story/download", {"story_pk": "17"}, "story_download", (17, ""), False),
        ("/story/download/by/url", {"url": "https://example.test/media"}, "story_download_by_url", ("https://example.test/media", ""), False),
        ("/album/download", {"media_pk": "17"}, "album_download", (17,), True),
        ("/album/download/by/urls", {"urls": ["https://example.test/media"]}, "album_download_by_urls", (["https://example.test/media"],), True),
        ("/track/download/by/url", {"url": "https://example.test/media"}, "track_download_by_url", ("https://example.test/media", ""), False),
    ],
)
@pytest.mark.parametrize(
    ("folder_query", "expected_folder"),
    [(None, Path(".")), ("", Path(".")), ("downloads/media", Path("downloads/media"))],
)
async def test_download_routes_preserve_typed_folder_defaults_and_explicit_queries(
    storage, monkeypatch, route, query, method, provider_args, returns_list, folder_query, expected_folder,
):
    calls = []

    async def download(*args):
        calls.append(args)
        return [Path("fictional-download")] if returns_list else Path("fictional-download")

    monkeypatch.setattr(storage.client, method, download, raising=False)
    params = {"sessionid": "sid", "returnFile": "false", **query}
    if folder_query is not None:
        params["folder"] = folder_query
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get(route, params=params)
    assert response.status_code == 200
    assert response.json() == (["fictional-download"] if returns_list else "fictional-download")
    assert calls == [(*provider_args, expected_folder)]
    assert isinstance(calls[0][-1], Path)

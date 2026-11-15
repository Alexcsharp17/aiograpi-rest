from pathlib import Path
from typing import List, Optional

from aiograpi.types import Media, Track
from fastapi import APIRouter, Depends, File, Form, Query, UploadFile

from aiograpi_rest.dependencies import ClientStorage, get_clients, get_sessionid
from aiograpi_rest.helpers import (
    LOCATION_FORM_DESCRIPTION,
    USERTAGS_FORM_DESCRIPTION,
    album_upload_post,
    album_upload_with_music_post,
    parse_json_form_dict,
    parse_json_form_model,
    parse_upload_location,
    parse_upload_usertags,
)

router = APIRouter(
    prefix="/album",
    tags=["Album (Carousel)"],
    responses={404: {"description": "Not found"}},
)
TRACK_FORM_DESCRIPTION = "JSON-encoded Track object returned by search or music browser endpoints."
EXTRA_DATA_FORM_DESCRIPTION = "JSON-encoded extra configure data. Leave empty to omit."


@router.get("/download", response_model=List[Path])
async def album_download(sessionid: str = Depends(get_sessionid),
                         media_pk: int = Query(...),
                         folder: Optional[Path] = Query(Path(".")),
                         clients: ClientStorage = Depends(get_clients)) -> List[Path]:
    """Download photo using media pk
    """
    cl = await clients.get(sessionid)
    result = await cl.album_download(media_pk, folder)
    return result


@router.get("/download/by/urls", response_model=List[Path])
async def album_download_by_urls(sessionid: str = Depends(get_sessionid),
                         urls: List[str] = Query(...),
                         folder: Optional[Path] = Query(Path(".")),
                         clients: ClientStorage = Depends(get_clients)) -> List[Path]:
    """Download photo using URL
    """
    cl = await clients.get(sessionid)
    result = await cl.album_download_by_urls(urls, folder)
    return result


@router.post("/upload", response_model=Media)
async def album_upload(sessionid: str = Depends(get_sessionid),
                       files: List[UploadFile] = File(...),
                       caption: str = Form(...),
                       usertags: Optional[List[str]] = Form([], description=USERTAGS_FORM_DESCRIPTION),
                       location: Optional[str] = Form(None, description=LOCATION_FORM_DESCRIPTION),
                       clients: ClientStorage = Depends(get_clients)
                       ) -> Media:
    """Upload album to feed
    """
    cl = await clients.get(sessionid)

    return await album_upload_post(
        cl, files, caption=caption,
        usertags=parse_upload_usertags(usertags),
        location=parse_upload_location(location))


@router.post("/upload/with/music", response_model=Media)
async def album_upload_with_music(sessionid: str = Depends(get_sessionid),
                       files: List[UploadFile] = File(...),
                       caption: str = Form(...),
                       track: str = Form(..., description=TRACK_FORM_DESCRIPTION),
                       usertags: Optional[List[str]] = Form([], description=USERTAGS_FORM_DESCRIPTION),
                       location: Optional[str] = Form(None, description=LOCATION_FORM_DESCRIPTION),
                       configure_timeout: int = Form(3),
                       extra_data: Optional[str] = Form(None, description=EXTRA_DATA_FORM_DESCRIPTION),
                       audio_asset_start_time: Optional[int] = Form(None),
                       overlap_duration: int = Form(30000),
                       browse_session_id: Optional[str] = Form(None),
                       alacorn_session_id: Optional[str] = Form(None),
                       clients: ClientStorage = Depends(get_clients)
                       ) -> Media:
    """Upload a carousel album with music
    """
    cl = await clients.get(sessionid)

    parsed_track = parse_json_form_model(track, Track, "track")
    parsed_extra_data = parse_json_form_dict(extra_data, "extra_data", default={})
    return await album_upload_with_music_post(
        cl, files, caption=caption,
        usertags=parse_upload_usertags(usertags),
        location=parse_upload_location(location),
        configure_timeout=configure_timeout,
        track=parsed_track,
        extra_data=parsed_extra_data,
        audio_asset_start_time=audio_asset_start_time,
        overlap_duration=overlap_duration,
        browse_session_id=browse_session_id,
        alacorn_session_id=alacorn_session_id)

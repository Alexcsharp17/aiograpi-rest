from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from aiograpi.types import Media, Track
from fastapi import APIRouter, Depends, File, Form, Query, UploadFile
from fastapi.responses import FileResponse

from aiograpi_rest.dependencies import ClientStorage, get_clients, get_sessionid
from aiograpi_rest.helpers import (
    LOCATION_FORM_DESCRIPTION,
    USERTAGS_FORM_DESCRIPTION,
    clip_upload_post,
    clip_upload_with_music_post,
    parse_json_form_dict,
    parse_json_form_model,
    parse_upload_location,
    parse_upload_usertags,
)

router = APIRouter(
    prefix="/clip",
    tags=["Clip (Reels)"],
    responses={404: {"description": "Not found"}},
)
DEVICE_STATUS_QUERY_DESCRIPTION = "JSON-encoded device status object. Leave empty to let aiograpi use defaults."
TRACK_FORM_DESCRIPTION = "JSON-encoded Track object returned by search or music browser endpoints."
EXTRA_DATA_FORM_DESCRIPTION = "JSON-encoded extra configure data. Leave empty to omit."


@router.get("/creation/info", response_model=Dict[str, Any])
async def clip_creation_info(
    sessionid: str = Depends(get_sessionid),
    clients: ClientStorage = Depends(get_clients),
) -> Dict[str, Any]:
    """Get Reel creation info
    """
    cl = await clients.get(sessionid)
    return await cl.clip_info_for_creation()


@router.get("/trial-eligibility", response_model=bool)
async def clip_trial_eligibility(
    sessionid: str = Depends(get_sessionid),
    clients: ClientStorage = Depends(get_clients),
) -> bool:
    """Check Trial Reels eligibility
    """
    cl = await clients.get(sessionid)
    return await cl.clip_trial_eligible()


@router.get("/share/facebook/config", response_model=Dict[str, Any])
async def clip_share_facebook_config(
    sessionid: str = Depends(get_sessionid),
    device_status: Optional[str] = Query(None, description=DEVICE_STATUS_QUERY_DESCRIPTION),
    clients: ClientStorage = Depends(get_clients),
) -> Dict[str, Any]:
    """Get Reel Facebook sharing config
    """
    cl = await clients.get(sessionid)
    parsed_device_status = parse_json_form_dict(device_status, "device_status", default=None)
    return await cl.clip_share_to_fb_config(parsed_device_status)


@router.post("/pin", response_model=bool)
async def clip_pin(
    sessionid: str = Depends(get_sessionid),
    media_pk: str = Form(...),
    clients: ClientStorage = Depends(get_clients),
) -> bool:
    """Pin a Reel
    """
    cl = await clients.get(sessionid)
    return await cl.clip_pin(media_pk)


@router.delete("/pin", response_model=bool)
async def clip_unpin(
    sessionid: str = Depends(get_sessionid),
    media_pk: str = Query(...),
    clients: ClientStorage = Depends(get_clients),
) -> bool:
    """Unpin a Reel
    """
    cl = await clients.get(sessionid)
    return await cl.clip_unpin(media_pk)


@router.get("/template", response_model=Dict[str, Any])
async def clip_template(
    sessionid: str = Depends(get_sessionid),
    media_id: str = Query(...),
    clients: ClientStorage = Depends(get_clients),
) -> Dict[str, Any]:
    """Get clip template
    """
    cl = await clients.get(sessionid)
    return await cl.media_template_v1(media_id)


@router.get("/download")
async def clip_download(sessionid: str = Depends(get_sessionid),
                         media_pk: int = Query(...),
                         folder: Optional[Path] = Query(Path(".")),
                         returnFile: Optional[bool] = Query(True),
                         clients: ClientStorage = Depends(get_clients)):
    """Download CLIP video using media pk
    """
    cl = await clients.get(sessionid)
    result = await cl.clip_download(media_pk, folder)
    if returnFile:
        return FileResponse(result)
    else:
        return result


@router.get("/download/by/url")
async def clip_download_by_url(sessionid: str = Depends(get_sessionid),
                         url: str = Query(...),
                         filename: Optional[str] = Query(""),
                         folder: Optional[Path] = Query(Path(".")),
                         returnFile: Optional[bool] = Query(True),
                         clients: ClientStorage = Depends(get_clients)):
    """Download CLIP video using URL
    """
    cl = await clients.get(sessionid)
    result = await cl.clip_download_by_url(url, filename, folder)
    if returnFile:
        return FileResponse(result)
    else:
        return result


@router.post("/upload", response_model=Media)
async def clip_upload(sessionid: str = Depends(get_sessionid),
                       file: UploadFile = File(...),
                       caption: str = Form(...),
                       thumbnail: Optional[UploadFile] = File(None),
                       usertags: Optional[List[str]] = Form([], description=USERTAGS_FORM_DESCRIPTION),
                       location: Optional[str] = Form(None, description=LOCATION_FORM_DESCRIPTION),
                       clients: ClientStorage = Depends(get_clients)
                       ) -> Media:
    """Upload photo and configure to feed
    """
    cl = await clients.get(sessionid)

    content = await file.read()
    usernames_tags = parse_upload_usertags(usertags)
    parsed_location = parse_upload_location(location)
    if thumbnail is not None:
        thumb = await thumbnail.read()
        return await clip_upload_post(
            cl, content, caption=caption,
            thumbnail=thumb,
            usertags=usernames_tags,
            location=parsed_location)
    return await clip_upload_post(
            cl, content, caption=caption,
            usertags=usernames_tags,
            location=parsed_location)


@router.post("/upload/with/music", response_model=Media)
async def clip_upload_with_music(
    sessionid: str = Depends(get_sessionid),
    file: UploadFile = File(...),
    caption: str = Form(...),
    track: str = Form(..., description=TRACK_FORM_DESCRIPTION),
    thumbnail: Optional[UploadFile] = File(None),
    usertags: Optional[List[str]] = Form([], description=USERTAGS_FORM_DESCRIPTION),
    location: Optional[str] = Form(None, description=LOCATION_FORM_DESCRIPTION),
    extra_data: Optional[str] = Form(None, description=EXTRA_DATA_FORM_DESCRIPTION),
    audio_asset_start_time: Optional[int] = Form(None),
    overlap_duration: int = Form(30000),
    original_volume: float = Form(1.0),
    music_volume: float = Form(1.0),
    product: str = Form("story_camera_clips_v2"),
    alacorn_session_id: str = Form("null"),
    clients: ClientStorage = Depends(get_clients)
) -> Media:
    """Upload a Reel with music
    """
    cl = await clients.get(sessionid)

    content = await file.read()
    parsed_track = parse_json_form_model(track, Track, "track")
    parsed_usertags = parse_upload_usertags(usertags)
    parsed_location = parse_upload_location(location)
    parsed_extra_data = parse_json_form_dict(extra_data, "extra_data", default={})
    thumbnail_content = await thumbnail.read() if thumbnail is not None else None
    return await clip_upload_with_music_post(
            cl, content, caption=caption,
            thumbnail=thumbnail_content,
            usertags=parsed_usertags,
            location=parsed_location,
            track=parsed_track,
            extra_data=parsed_extra_data,
            audio_asset_start_time=audio_asset_start_time,
            overlap_duration=overlap_duration,
            original_volume=original_volume,
            music_volume=music_volume,
            product=product,
            alacorn_session_id=alacorn_session_id)


@router.post("/upload/by/url", response_model=Media)
async def clip_upload(sessionid: str = Depends(get_sessionid),
                       url: str = Form(...),
                       caption: str = Form(...),
                       thumbnail: Optional[UploadFile] = File(None),
                       usertags: Optional[List[str]] = Form([], description=USERTAGS_FORM_DESCRIPTION),
                       location: Optional[str] = Form(None, description=LOCATION_FORM_DESCRIPTION),
                       clients: ClientStorage = Depends(get_clients)
                       ) -> Media:
    """Upload photo by URL and configure to feed
    """
    cl = await clients.get(sessionid)

    content = requests.get(url).content
    usernames_tags = parse_upload_usertags(usertags)
    parsed_location = parse_upload_location(location)
    if thumbnail is not None:
        thumb = await thumbnail.read()
        return await clip_upload_post(
            cl, content, caption=caption,
            thumbnail=thumb,
            usertags=usernames_tags,
            location=parsed_location)
    return await clip_upload_post(
            cl, content, caption=caption,
            usertags=usernames_tags,
            location=parsed_location)

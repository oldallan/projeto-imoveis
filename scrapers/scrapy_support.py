from __future__ import annotations

import threading
from typing import Any

import scrapy
from curl_cffi import requests
from scrapy.http import Headers, HtmlResponse, Request, Response
from twisted.internet.threads import deferToThread


class CurlCffiDownloadHandler:
    lazy = False
    _STRIP_RESPONSE_HEADERS = {"content-encoding", "content-length", "transfer-encoding"}

    def __init__(self, settings: Any, crawler: Any) -> None:
        self.settings = settings
        self.crawler = crawler
        self.timeout = settings.getfloat("DOWNLOAD_TIMEOUT", 30.0)
        self.impersonate = settings.get("CURL_CFFI_IMPERSONATE", "chrome110")
        self.verify = settings.getbool("CURL_CFFI_VERIFY", True)
        self.thread_local = threading.local()

    @classmethod
    def from_crawler(cls, crawler: Any) -> "CurlCffiDownloadHandler":
        return cls(crawler.settings, crawler)

    def _get_session(self) -> requests.Session:
        session = getattr(self.thread_local, "session", None)
        if session is None:
            session = requests.Session()
            self.thread_local.session = session
        return session

    def _request_headers_to_dict(self, request: Request) -> dict[str, str]:
        converted: dict[str, str] = {}
        for key in request.headers.keys():
            key_text = key.decode("latin1") if isinstance(key, bytes) else str(key)
            values = request.headers.getlist(key)
            converted[key_text] = ", ".join(
                value.decode("latin1") if isinstance(value, bytes) else str(value)
                for value in values
            )
        return converted

    def _response_headers(self, response: Any) -> Headers:
        headers = Headers()
        for key, value in response.headers.items():
            key_text = str(key)
            if key_text.lower() in self._STRIP_RESPONSE_HEADERS:
                continue
            headers.appendlist(key_text, str(value))
        return headers

    def _build_scrapy_response(self, request: Request, response: Any) -> Response:
        body = response.content or b""
        headers = self._response_headers(response)
        headers["Content-Length"] = str(len(body))
        content_type = str(response.headers.get("content-type", "")).lower()
        response_url = str(response.url or request.url)
        is_html = "html" in content_type or b"<html" in body[:2048].lower()
        if is_html:
            return HtmlResponse(
                url=response_url,
                status=response.status_code,
                headers=headers,
                body=body,
                encoding=response.encoding or "utf-8",
                request=request,
            )
        return Response(
            url=response_url,
            status=response.status_code,
            headers=headers,
            body=body,
            request=request,
        )

    def _download_request_sync(self, request: Request) -> Response:
        session = self._get_session()
        response = session.request(
            method=request.method,
            url=request.url,
            headers=self._request_headers_to_dict(request),
            data=request.body or None,
            timeout=request.meta.get("download_timeout", self.timeout),
            impersonate=self.impersonate,
            allow_redirects=not request.meta.get("dont_redirect", False),
            verify=self.verify,
        )
        return self._build_scrapy_response(request, response)

    def download_request(self, request: Request, spider: scrapy.Spider):
        return deferToThread(self._download_request_sync, request)

    def close(self) -> None:
        session = getattr(self.thread_local, "session", None)
        if session is not None:
            session.close()
            self.thread_local.session = None


def build_scrapy_settings(
    *,
    user_agent: str,
    default_headers: dict[str, str],
    verbose: bool = False,
    retry_times: int = 2,
    autothrottle_start_delay: float = 1.0,
    autothrottle_max_delay: float = 8.0,
    autothrottle_target_concurrency: float = 1.0,
    concurrent_requests: int = 2,
    concurrent_requests_per_domain: int = 1,
    download_delay: float = 1.0,
    randomize_download_delay: bool = True,
    download_timeout: int = 30,
    impersonate: str = "chrome110",
    jobdir: str | None = None,
) -> dict[str, Any]:
    log_level = "INFO" if verbose else "WARNING"
    settings = {
        "USER_AGENT": user_agent,
        "DEFAULT_REQUEST_HEADERS": default_headers,
        "ROBOTSTXT_OBEY": False,
        "COOKIES_ENABLED": False,
        "TELNETCONSOLE_ENABLED": False,
        "LOG_LEVEL": log_level,
        "RETRY_ENABLED": True,
        "RETRY_TIMES": retry_times,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": autothrottle_start_delay,
        "AUTOTHROTTLE_MAX_DELAY": autothrottle_max_delay,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": autothrottle_target_concurrency,
        "CONCURRENT_REQUESTS": concurrent_requests,
        "CONCURRENT_REQUESTS_PER_DOMAIN": concurrent_requests_per_domain,
        "DOWNLOAD_DELAY": download_delay,
        "RANDOMIZE_DOWNLOAD_DELAY": randomize_download_delay,
        "DOWNLOAD_TIMEOUT": download_timeout,
        "DOWNLOAD_HANDLERS": {
            "http": "scrapers.scrapy_support.CurlCffiDownloadHandler",
            "https": "scrapers.scrapy_support.CurlCffiDownloadHandler",
        },
        "CURL_CFFI_IMPERSONATE": impersonate,
    }
    if jobdir:
        settings["JOBDIR"] = jobdir
    return settings

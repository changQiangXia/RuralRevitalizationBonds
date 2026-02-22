#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Rural revitalization bond crawler and monthly aggregation pipeline.

Data scope:
- Exchanges: SSE + SZSE
- Date window: configurable (default 2024-01-01 ~ 2025-12-31)
- Keyword: configurable (default "乡村振兴")

Outputs:
- bond_detail_raw.csv
- bond_detail_clean.csv
- monthly_total.csv
- monthly_by_exchange.csv
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import datetime as dt
import hashlib
import json
import logging
import re
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from bs4 import BeautifulSoup
from pypdf import PdfReader


SSE_BASE = "https://www.sse.com.cn"
SSE_QUERY = "https://query.sse.com.cn/commonSoaQuery.do"
SZSE_BASE = "https://www.szse.cn"
SZSE_SEARCH_API = "https://www.szse.cn/api/search/content"

AMOUNT_PRIORITY = {"actual": 3, "scale": 2, "total": 1, None: 0}
RESULT_DATE_SOURCES = {
    "sse_result_sentence",
    "szse_result_sentence",
}

SSE_RESULT_TITLE_HINTS = (
    "发行结果公告",
    "发行情况公告",
    "发行完成公告",
    "发行结果的公告",
)


def setup_logger() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("pypdf").setLevel(logging.ERROR)
    logging.getLogger("matplotlib").setLevel(logging.WARNING)
    return logging.getLogger("rural_bonds")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Crawl SSE/SZSE rural revitalization bond data and aggregate monthly issuance amount."
    )
    parser.add_argument("--start-date", default="2024-01-01", help="YYYY-MM-DD")
    parser.add_argument("--end-date", default="2025-12-31", help="YYYY-MM-DD")
    parser.add_argument("--keyword", default="乡村振兴", help="Search keyword")
    parser.add_argument("--output-dir", default="output", help="Output directory")
    parser.add_argument("--cache-dir", default=".cache", help="Cache directory")
    parser.add_argument("--max-workers", type=int, default=8, help="Thread workers for detail parsing")
    parser.add_argument("--sse-page-size", type=int, default=100, help="SSE query page size")
    parser.add_argument("--szse-page-size", type=int, default=100, help="SZSE query page size")
    return parser.parse_args()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", "", text or "")


def strip_html_tags(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"<[^>]+>", "", text)


def safe_float(num_text: str) -> float | None:
    if not num_text:
        return None
    cleaned = re.sub(r"[,\s]", "", num_text)
    try:
        return float(cleaned)
    except ValueError:
        return None


def date_to_str(d: dt.date) -> str:
    return d.strftime("%Y-%m-%d")


def parse_date_string(value: str) -> dt.date:
    return dt.datetime.strptime(value, "%Y-%m-%d").date()


def in_date_window(date_str: str, start: dt.date, end: dt.date) -> bool:
    if not date_str:
        return False
    try:
        d = parse_date_string(date_str)
    except ValueError:
        return False
    return start <= d <= end


def read_json_url(url: str, headers: dict[str, str] | None = None, retries: int = 3, timeout: int = 30) -> dict[str, Any]:
    headers = headers or {}
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                body = resp.read().decode("utf-8", errors="ignore")
                return json.loads(body)
        except Exception:
            if i == retries - 1:
                raise
            time.sleep(1.2 * (i + 1))
    return {}


def get_requests_text(
    session: requests.Session,
    url: str,
    *,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    data: dict[str, Any] | None = None,
    retries: int = 3,
    timeout: int = 30,
) -> str:
    headers = headers or {}
    for i in range(retries):
        try:
            if method.upper() == "POST":
                resp = session.post(url, headers=headers, data=data, timeout=timeout)
            else:
                resp = session.get(url, headers=headers, timeout=timeout)
            resp.raise_for_status()
            return resp.text
        except Exception:
            if i == retries - 1:
                raise
            time.sleep(1.2 * (i + 1))
    return ""


def get_requests_json(
    session: requests.Session,
    url: str,
    *,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    data: dict[str, Any] | None = None,
    retries: int = 3,
    timeout: int = 30,
) -> dict[str, Any]:
    text = get_requests_text(
        session,
        url,
        method=method,
        headers=headers,
        data=data,
        retries=retries,
        timeout=timeout,
    )
    return json.loads(text)


def convert_cn_digit(ch: str) -> int:
    mp = {
        "零": 0,
        "〇": 0,
        "一": 1,
        "二": 2,
        "三": 3,
        "四": 4,
        "五": 5,
        "六": 6,
        "七": 7,
        "八": 8,
        "九": 9,
    }
    return mp.get(ch, -1)


def convert_cn_number(text: str) -> int | None:
    if not text:
        return None
    if "十" in text:
        if text == "十":
            return 10
        if text.startswith("十"):
            tail = convert_cn_digit(text[1])
            return 10 + tail if tail >= 0 else None
        if text.endswith("十"):
            head = convert_cn_digit(text[0])
            return head * 10 if head >= 0 else None
        if len(text) == 3 and text[1] == "十":
            head = convert_cn_digit(text[0])
            tail = convert_cn_digit(text[2])
            if head >= 0 and tail >= 0:
                return head * 10 + tail
            return None
        return None
    digits = []
    for ch in text:
        d = convert_cn_digit(ch)
        if d < 0:
            return None
        digits.append(str(d))
    try:
        return int("".join(digits))
    except ValueError:
        return None


def extract_cn_date(text: str) -> str | None:
    pat = re.compile(r"([〇零一二三四五六七八九]{4})年([〇零一二三四五六七八九十]{1,3})月([〇零一二三四五六七八九十]{1,3})日")
    m = pat.search(text or "")
    if not m:
        return None
    y_text, m_text, d_text = m.groups()
    y_digits = []
    for ch in y_text:
        d = convert_cn_digit(ch)
        if d < 0:
            return None
        y_digits.append(str(d))
    try:
        year = int("".join(y_digits))
    except ValueError:
        return None
    month = convert_cn_number(m_text)
    day = convert_cn_number(d_text)
    if not month or not day:
        return None
    try:
        return date_to_str(dt.date(year, month, day))
    except ValueError:
        return None


def extract_amount(text: str) -> tuple[float | None, str | None]:
    if not text:
        return None, None
    squashed = normalize_whitespace(text)
    patterns = [
        ("actual", r"(?:实际发行金额|实际发行规模|最终发行金额|最终发行规模)[为：:]?([0-9][0-9,.\s]*)亿元", "yi"),
        ("actual", r"(?:实际发行金额|实际发行规模|最终发行金额|最终发行规模)[为：:]?([0-9][0-9,.\s]*)万元", "wan"),
        ("scale", r"(?:发行规模|本期债券发行规模|本期债券发行金额)(?:不超过|为|不低于|上限为|下限为|拟为|约为)?([0-9][0-9,.\s]*)亿元", "yi"),
        ("scale", r"(?:发行规模|本期债券发行规模|本期债券发行金额)(?:不超过|为|不低于|上限为|下限为|拟为|约为)?([0-9][0-9,.\s]*)万元", "wan"),
        ("total", r"(?:发行总额|债券发行总额)[为：:]?([0-9][0-9,.\s]*)亿元", "yi"),
        ("total", r"(?:发行总额|债券发行总额)[为：:]?([0-9][0-9,.\s]*)万元", "wan"),
    ]
    best_amount: float | None = None
    best_type: str | None = None
    best_score = -1
    for amount_type, pattern, unit in patterns:
        m = re.search(pattern, squashed)
        if not m:
            continue
        value = safe_float(m.group(1))
        if value is None:
            continue
        if unit == "wan":
            value = value / 10000.0
        score = AMOUNT_PRIORITY[amount_type]
        if score > best_score:
            best_score = score
            best_amount = value
            best_type = amount_type
    return best_amount, best_type


def extract_issue_result_date(text: str, publish_date: str, exchange: str) -> tuple[str, str]:
    squashed = normalize_whitespace(text)
    patterns = [
        ("result_end_sentence", r"(?:发行工作已于|发行工作于|发行已于|发行于)(20\d{2})年(\d{1,2})月(\d{1,2})日(?:结束|完成)"),
        ("result_end_sentence", r"(?:本期债券发行工作已于|本期债券发行已于)(20\d{2})年(\d{1,2})月(\d{1,2})日(?:结束|完成)"),
        ("szse_listing_start_sentence", r"(?:定于)(20\d{2})年(\d{1,2})月(\d{1,2})日(?:起|开始)"),
    ]
    for source, pat in patterns:
        m = re.search(pat, squashed)
        if m:
            y, mm, dd = m.groups()
            try:
                d = dt.date(int(y), int(mm), int(dd))
                if source == "result_end_sentence":
                    return date_to_str(d), f"{exchange.lower()}_result_sentence"
                return date_to_str(d), source
            except ValueError:
                pass
    cn_date = extract_cn_date(text)
    if cn_date:
        src = f"{exchange.lower()}_cn_footer_date"
        return cn_date, src
    return publish_date, "publish_date_fallback"


def extract_security_info(text: str) -> tuple[str, str]:
    squashed = normalize_whitespace(text)
    code_match = re.search(r"证券代码[“\"']?([0-9]{6})", squashed)
    abbr_match = re.search(r"证券简称[“\"']?([^”\"'，。；;\s]{2,24})", squashed)
    code = code_match.group(1) if code_match else ""
    abbr = abbr_match.group(1) if abbr_match else ""
    return code, abbr


def cache_file_path(cache_dir: Path, url: str) -> Path:
    parsed = urllib.parse.urlparse(url)
    name = Path(parsed.path).name
    if not name:
        name = hashlib.md5(url.encode("utf-8")).hexdigest()
    hash_prefix = hashlib.md5(url.encode("utf-8")).hexdigest()[:10]
    return cache_dir / f"{hash_prefix}_{name}"


def download_file(session: requests.Session, url: str, dest: Path, headers: dict[str, str]) -> Path:
    if dest.exists() and dest.stat().st_size > 0:
        return dest
    for i in range(3):
        try:
            resp = session.get(url, headers=headers, timeout=40)
            resp.raise_for_status()
            dest.write_bytes(resp.content)
            return dest
        except Exception:
            if i == 2:
                raise
            time.sleep(1.2 * (i + 1))
    return dest


def extract_pdf_text(path: Path, max_pages: int = 8) -> str:
    reader = PdfReader(str(path))
    parts = []
    for page in reader.pages[:max_pages]:
        try:
            parts.append(page.extract_text() or "")
        except Exception:
            parts.append("")
    return "\n".join(parts)


def build_sse_query_url(
    *,
    keyword: str,
    start_date: str,
    end_date: str,
    page_no: int,
    page_size: int,
) -> str:
    params = {
        "isPagination": "true",
        "pageHelp.pageSize": str(page_size),
        "pageHelp.cacheSize": "1",
        "pageHelp.pageNo": str(page_no),
        "pageHelp.beginPage": str(page_no),
        "pageHelp.endPage": str(page_no),
        "type": "inParams",
        "sqlId": "BS_ZQ_GGLL",
        "sseDate": f"{start_date} 00:00:00",
        "sseDateEnd": f"{end_date} 23:59:59",
        "securityCode": "",
        "title": keyword,
        "orgBulletinType": "",
        "bondType": "COMPANY_BOND_BULLETIN",
        "order": "sseDate|desc,securityCode|asc,bulletinId|asc",
    }
    query = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
    return f"{SSE_QUERY}?{query}"


def fetch_sse_announcements(keyword: str, start_date: str, end_date: str, page_size: int, logger: logging.Logger) -> list[dict[str, Any]]:
    headers = {
        "Referer": "https://www.sse.com.cn/disclosure/bond/announcement/company/",
        "User-Agent": "Mozilla/5.0",
    }
    page_no = 1
    all_rows: list[dict[str, Any]] = []
    while True:
        url = build_sse_query_url(
            keyword=keyword,
            start_date=start_date,
            end_date=end_date,
            page_no=page_no,
            page_size=page_size,
        )
        data = read_json_url(url, headers=headers)
        page_rows = data.get("result") or []
        if not isinstance(page_rows, list):
            page_rows = []
        all_rows.extend(page_rows)
        page_help = data.get("pageHelp") or {}
        total = int(page_help.get("total") or 0)
        page_count = int(page_help.get("pageCount") or 0)
        logger.info("SSE page %s fetched: %s rows (total=%s)", page_no, len(page_rows), total)
        if page_count <= 0:
            break
        if page_no >= page_count:
            break
        page_no += 1
    logger.info("SSE total rows fetched: %s", len(all_rows))
    return all_rows


def fetch_szse_announcements(session: requests.Session, keyword: str, page_size: int, logger: logging.Logger) -> list[dict[str, Any]]:
    headers = {
        "Referer": "https://www.szse.cn/disclosure/notice/bond/",
        "User-Agent": "Mozilla/5.0",
    }
    page_no = 1
    all_rows: list[dict[str, Any]] = []
    total_size = None
    while True:
        payload = {
            "keyword": keyword,
            "time": 0,
            "range": "title",
            "channelCode": "bond_news",
            "currentPage": page_no,
            "pageSize": page_size,
        }
        data = get_requests_json(
            session,
            SZSE_SEARCH_API,
            method="POST",
            headers=headers,
            data=payload,
        )
        page_rows = data.get("data") or []
        if not isinstance(page_rows, list):
            page_rows = []
        total_size = int(data.get("totalSize") or 0)
        all_rows.extend(page_rows)
        logger.info("SZSE page %s fetched: %s rows (total=%s)", page_no, len(page_rows), total_size)
        if len(all_rows) >= total_size:
            break
        if not page_rows:
            break
        page_no += 1
    logger.info("SZSE total rows fetched: %s", len(all_rows))
    return all_rows


def parse_sse_detail(
    session: requests.Session,
    row: dict[str, Any],
    cache_pdf_dir: Path,
) -> dict[str, Any]:
    title = row.get("title") or ""
    url = row.get("url") or ""
    security_code = row.get("securityCode") or ""
    security_abbr = row.get("securityAbbr") or ""
    publish_date = (row.get("sseDate") or "").split(" ")[0]
    bulletin_type_code = row.get("orgBulletinType") or ""
    bulletin_type_name = row.get("orgBulletinTypeDesc") or ""
    if url.startswith("/"):
        url = SSE_BASE + url

    note = ""
    text = ""
    try:
        if url.lower().endswith(".pdf"):
            cache_path = cache_file_path(cache_pdf_dir, url)
            download_file(
                session,
                url,
                cache_path,
                headers={"Referer": "https://www.sse.com.cn/", "User-Agent": "Mozilla/5.0"},
            )
            text = extract_pdf_text(cache_path)
        else:
            html = get_requests_text(
                session,
                url,
                headers={"Referer": "https://www.sse.com.cn/", "User-Agent": "Mozilla/5.0"},
            )
            text = BeautifulSoup(html, "lxml").get_text(" ", strip=True)
    except Exception as exc:
        note = f"detail_parse_failed:{type(exc).__name__}"

    amount, amount_source = extract_amount(text)
    issue_date, issue_date_source = extract_issue_result_date(text, publish_date, "SSE")
    if not security_code or not security_abbr:
        code_from_text, abbr_from_text = extract_security_info(text)
        security_code = security_code or code_from_text
        security_abbr = security_abbr or abbr_from_text

    row_out = {
        "exchange": "SSE",
        "raw_id": row.get("bulletinId") or "",
        "security_code": security_code,
        "security_abbr": security_abbr,
        "bond_name": title,
        "title": title,
        "publish_date": publish_date,
        "issue_result_date": issue_date,
        "issue_date_source": issue_date_source,
        "amount_billion": amount,
        "amount_source": amount_source,
        "bulletin_type_code": bulletin_type_code,
        "bulletin_type_name": bulletin_type_name,
        "url": url,
        "parse_note": note,
    }
    return row_out


def parse_szse_detail(session: requests.Session, row: dict[str, Any]) -> dict[str, Any]:
    title = strip_html_tags(row.get("doctitle") or "")
    url = row.get("docpuburl") or ""
    docpubjsonurl = row.get("docpubjsonurl") or ""
    publish_ts = int(row.get("docpubtime") or 0)
    publish_date = ""
    if publish_ts > 0:
        publish_date = dt.datetime.fromtimestamp(publish_ts / 1000).strftime("%Y-%m-%d")

    if url and url.startswith("/"):
        url = SZSE_BASE + url
    if docpubjsonurl and docpubjsonurl.startswith("/"):
        detail_url = SZSE_BASE + docpubjsonurl
    else:
        detail_url = docpubjsonurl

    note = ""
    text = ""
    detail_title = title
    try:
        detail_data = get_requests_json(
            session,
            detail_url,
            method="GET",
            headers={"Referer": "https://www.szse.cn/disclosure/notice/bond/", "User-Agent": "Mozilla/5.0"},
        )
        content_html = (((detail_data.get("data") or {}).get("content")) or "")
        detail_title = ((detail_data.get("data") or {}).get("title")) or detail_title
        detail_title = strip_html_tags(detail_title)
        text = BeautifulSoup(content_html, "lxml").get_text(" ", strip=True)
        pub_ts = ((detail_data.get("data") or {}).get("pubTime")) or 0
        if pub_ts and not publish_date:
            publish_date = dt.datetime.fromtimestamp(int(pub_ts) / 1000).strftime("%Y-%m-%d")
    except Exception as exc:
        note = f"detail_parse_failed:{type(exc).__name__}"

    amount, amount_source = extract_amount(text)
    issue_date, issue_date_source = extract_issue_result_date(text, publish_date, "SZSE")
    security_code, security_abbr = extract_security_info(text)

    return {
        "exchange": "SZSE",
        "raw_id": row.get("id") or "",
        "security_code": security_code,
        "security_abbr": security_abbr,
        "bond_name": detail_title,
        "title": detail_title,
        "publish_date": publish_date,
        "issue_result_date": issue_date,
        "issue_date_source": issue_date_source,
        "amount_billion": amount,
        "amount_source": amount_source,
        "bulletin_type_code": "",
        "bulletin_type_name": "",
        "url": url,
        "parse_note": note,
    }


def normalize_security_code(code: str | int | float | None) -> str:
    if code is None:
        return ""
    s = str(code).strip()
    if not s:
        return ""
    m = re.search(r"(\d{6})", s)
    return m.group(1) if m else ""


def normalize_security_abbr(abbr: str | None) -> str:
    if not abbr:
        return ""
    return strip_html_tags(str(abbr)).strip()


def extract_issuer_from_title(title: str) -> str:
    t = strip_html_tags(title or "")
    t = re.sub(r"\s+", "", t)
    patterns = [
        r"关于为(.+?)20\d{2}年",
        r"关于(.+?)20\d{2}年",
        r"(.+?)20\d{2}年",
    ]
    for pat in patterns:
        m = re.search(pat, t)
        if m:
            issuer = m.group(1).strip("，。:：")
            issuer = issuer.replace("提供转让服务有关事项的通知", "")
            issuer = issuer.replace("上市有关事项的通知", "")
            issuer = issuer.replace("关于", "")
            if len(issuer) >= 4:
                return issuer
    return ""


def szse_search_global(
    session: requests.Session,
    keyword: str,
    *,
    page_size: int = 50,
) -> list[dict[str, Any]]:
    if not keyword.strip():
        return []
    payload = {
        "keyword": keyword,
        "time": 0,
        "range": "title",
        "currentPage": 1,
        "pageSize": page_size,
    }
    headers = {
        "Referer": "https://www.szse.cn/",
        "User-Agent": "Mozilla/5.0",
    }
    data = get_requests_json(
        session,
        SZSE_SEARCH_API,
        method="POST",
        headers=headers,
        data=payload,
    )
    rows = data.get("data") or []
    if not isinstance(rows, list):
        return []
    return rows


def clean_search_title(text: str) -> str:
    return strip_html_tags(text or "")


def score_szse_result_candidate(
    candidate: dict[str, Any],
    *,
    security_code: str,
    security_abbr: str,
    issuer: str,
    reference_date: str,
) -> int:
    title = clean_search_title(candidate.get("doctitle") or "")
    title_n = normalize_whitespace(title)
    score = 0
    if "发行结果公告" in title_n:
        score += 6
    if "发行情况公告" in title_n:
        score += 4
    if "更正公告" in title_n:
        score -= 3
    if security_abbr and security_abbr in title:
        score += 6
    if security_code and security_code in title:
        score += 6
    if issuer and issuer in title:
        score += 4
    if "乡村振兴" in title:
        score += 2
    chnl = (candidate.get("chnlcode") or "").strip()
    if chnl in {"companyDebt_disc", "bondinfoNotice_disc"}:
        score += 2

    ref = reference_date
    ts = int(candidate.get("docpubtime") or 0)
    if ref and ts > 0:
        d = dt.datetime.fromtimestamp(ts / 1000).date()
        try:
            ref_d = parse_date_string(ref)
            delta = abs((ref_d - d).days)
            if delta <= 15:
                score += 3
            elif delta <= 45:
                score += 1
            elif delta > 180:
                score -= 2
        except ValueError:
            pass
    return score


def find_szse_result_announcement(
    session: requests.Session,
    row: dict[str, Any],
) -> dict[str, Any] | None:
    security_code = normalize_security_code(row.get("security_code"))
    security_abbr = normalize_security_abbr(row.get("security_abbr"))
    issuer = extract_issuer_from_title(row.get("title") or "")
    reference_date = str(row.get("publish_date") or "")

    query_candidates: list[str] = []
    if security_abbr:
        query_candidates.append(f"{security_abbr} 发行结果公告")
        query_candidates.append(f"{security_abbr} 发行情况公告")
    if issuer:
        query_candidates.append(f"{issuer} 乡村振兴 发行结果公告")
        query_candidates.append(f"{issuer} 发行结果公告")
    if security_code:
        query_candidates.append(f"{security_code} 发行结果公告")

    seen = set()
    query_candidates = [q for q in query_candidates if not (q in seen or seen.add(q))]

    all_hits: list[dict[str, Any]] = []
    for query in query_candidates:
        try:
            hits = szse_search_global(session, query)
        except Exception:
            continue
        all_hits.extend(hits)
        if len(all_hits) >= 120:
            break

    if not all_hits:
        return None

    dedup: dict[str, dict[str, Any]] = {}
    for item in all_hits:
        key = item.get("docpuburl") or f"id:{item.get('id')}"
        if key not in dedup:
            dedup[key] = item

    scored: list[tuple[int, dict[str, Any]]] = []
    for item in dedup.values():
        title = clean_search_title(item.get("doctitle") or "")
        # keep only result-related announcements
        if ("发行结果公告" not in title) and ("发行情况公告" not in title):
            continue
        s = score_szse_result_candidate(
            item,
            security_code=security_code,
            security_abbr=security_abbr,
            issuer=issuer,
            reference_date=reference_date,
        )
        scored.append((s, item))

    if not scored:
        return None

    scored.sort(key=lambda x: x[0], reverse=True)
    best_score, best_item = scored[0]
    if best_score < 6:
        return None
    return best_item


def parse_szse_result_announcement(
    session: requests.Session,
    item: dict[str, Any],
    cache_pdf_dir: Path,
) -> dict[str, Any]:
    url = item.get("docpuburl") or ""
    if url.startswith("/"):
        url = SZSE_BASE + url
    title = clean_search_title(item.get("doctitle") or "")

    pub_ts = int(item.get("docpubtime") or 0)
    publish_date = ""
    if pub_ts > 0:
        publish_date = dt.datetime.fromtimestamp(pub_ts / 1000).strftime("%Y-%m-%d")

    text = ""
    if url.lower().endswith(".pdf"):
        cache_path = cache_file_path(cache_pdf_dir, url)
        download_file(
            session,
            url,
            cache_path,
            headers={"Referer": "https://www.szse.cn/", "User-Agent": "Mozilla/5.0"},
        )
        text = extract_pdf_text(cache_path)
    else:
        html = get_requests_text(
            session,
            url,
            headers={"Referer": "https://www.szse.cn/", "User-Agent": "Mozilla/5.0"},
        )
        text = BeautifulSoup(html, "lxml").get_text(" ", strip=True)

    issue_date, issue_source = extract_issue_result_date(text, publish_date, "SZSE")
    if issue_source == "publish_date_fallback":
        issue_source = "szse_result_announcement_publish_date"
    amount, amount_source = extract_amount(text)

    return {
        "result_url": url,
        "result_title": title,
        "result_publish_date": publish_date,
        "issue_result_date": issue_date,
        "issue_date_source": issue_source,
        "amount_billion": amount,
        "amount_source": amount_source,
    }


def enhance_szse_rows_with_result_announcements(
    session: requests.Session,
    rows: list[dict[str, Any]],
    cache_pdf_dir: Path,
    max_workers: int,
    logger: logging.Logger,
) -> list[dict[str, Any]]:
    targets = [r for r in rows if r.get("issue_date_source") == "szse_listing_start_sentence"]
    if not targets:
        return rows

    logger.info("SZSE secondary trace targets: %s", len(targets))

    found_map: dict[int, dict[str, Any]] = {}
    for idx, row in enumerate(rows):
        if row.get("issue_date_source") != "szse_listing_start_sentence":
            continue
        try:
            item = find_szse_result_announcement(session, row)
            if item:
                found_map[idx] = item
        except Exception:
            continue

    logger.info("SZSE secondary trace matched announcements: %s", len(found_map))
    if not found_map:
        return rows

    parsed_map: dict[int, dict[str, Any]] = {}
    with cf.ThreadPoolExecutor(max_workers=max_workers) as ex:
        future_idx = {
            ex.submit(parse_szse_result_announcement, session, item, cache_pdf_dir): idx
            for idx, item in found_map.items()
        }
        for fut in cf.as_completed(future_idx):
            idx = future_idx[fut]
            try:
                parsed_map[idx] = fut.result()
            except Exception:
                continue

    improved_date = 0
    improved_amount = 0
    for idx, info in parsed_map.items():
        row = rows[idx]
        old_source = row.get("issue_date_source")
        new_date = info.get("issue_result_date")
        new_src = info.get("issue_date_source")
        if new_date and new_src and new_src != old_source:
            row["issue_result_date"] = new_date
            row["issue_date_source"] = new_src
            improved_date += 1

        old_amount_src = row.get("amount_source")
        old_amount = row.get("amount_billion")
        new_amount = info.get("amount_billion")
        new_amount_src = info.get("amount_source")
        if new_amount is not None:
            old_score = AMOUNT_PRIORITY.get(old_amount_src, 0)
            new_score = AMOUNT_PRIORITY.get(new_amount_src, 0)
            if (old_amount is None) or (new_score > old_score):
                row["amount_billion"] = new_amount
                row["amount_source"] = new_amount_src
                improved_amount += 1

        row["trace_result_url"] = info.get("result_url", "")
        row["trace_result_title"] = info.get("result_title", "")
        row["trace_result_publish_date"] = info.get("result_publish_date", "")

    logger.info(
        "SZSE secondary trace updated: date=%s, amount=%s",
        improved_date,
        improved_amount,
    )
    return rows


def title_norm(title: str) -> str:
    t = strip_html_tags(title or "")
    t = normalize_whitespace(t)
    t = re.sub(r"[（）()【】\[\]，。,:：;；\-_]", "", t)
    return t


def dedupe_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        security_code = (r.get("security_code") or "").strip()
        if security_code:
            key = f"{r.get('exchange')}|{security_code}"
        else:
            key = f"{r.get('exchange')}|{title_norm(r.get('title') or '')}|{r.get('issue_result_date') or ''}"
        groups.setdefault(key, []).append(r)

    cleaned: list[dict[str, Any]] = []
    for _, candidates in groups.items():
        best = None
        best_score = -999
        for r in candidates:
            amount_score = AMOUNT_PRIORITY.get(r.get("amount_source"), 0)
            date_score = 2 if r.get("issue_date_source") in RESULT_DATE_SOURCES else 1
            data_score = 1 if r.get("amount_billion") is not None else -3
            score = amount_score * 10 + date_score * 2 + data_score
            if score > best_score:
                best_score = score
                best = r
        if best:
            cleaned.append(best)
    return cleaned


def build_monthly_outputs(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    monthly_total = (
        df.groupby("issue_month", as_index=False)["amount_billion"]
        .sum()
        .sort_values("issue_month")
        .rename(columns={"amount_billion": "monthly_amount_billion"})
    )
    monthly_by_exchange = (
        df.groupby(["issue_month", "exchange"], as_index=False)["amount_billion"]
        .sum()
        .sort_values(["issue_month", "exchange"])
        .rename(columns={"amount_billion": "monthly_amount_billion"})
    )
    return monthly_total, monthly_by_exchange


def save_monthly_chart(monthly_total: pd.DataFrame, output_path: Path) -> None:
    if monthly_total.empty:
        return
    import matplotlib.pyplot as plt

    x = monthly_total["issue_month"].astype(str).tolist()
    y = monthly_total["monthly_amount_billion"].astype(float).tolist()

    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(x, y, marker="o", linewidth=2.0, color="#2F6BFF")
    ax.set_xlabel("Month")
    ax.set_ylabel("Issuance Amount (Billion CNY)")
    ax.set_title("Monthly Rural Revitalization Bond Issuance")
    ax.grid(axis="y", linestyle="--", alpha=0.35)
    ax.tick_params(axis="x", rotation=45)
    fig.tight_layout()
    fig.savefig(output_path, dpi=180)
    plt.close(fig)


def run(args: argparse.Namespace) -> None:
    logger = setup_logger()
    start = parse_date_string(args.start_date)
    end = parse_date_string(args.end_date)
    if start > end:
        raise ValueError("start-date must be <= end-date")

    output_dir = Path(args.output_dir)
    cache_dir = Path(args.cache_dir)
    cache_pdf_dir = cache_dir / "sse_pdf"
    cache_szse_pdf_dir = cache_dir / "szse_pdf"
    ensure_dir(output_dir)
    ensure_dir(cache_pdf_dir)
    ensure_dir(cache_szse_pdf_dir)

    session = requests.Session()

    logger.info("Step 1/5: Fetch SSE announcements")
    sse_rows = fetch_sse_announcements(
        keyword=args.keyword,
        start_date=args.start_date,
        end_date=args.end_date,
        page_size=args.sse_page_size,
        logger=logger,
    )

    logger.info("Step 2/5: Fetch SZSE announcements")
    szse_rows = fetch_szse_announcements(
        session=session,
        keyword=args.keyword,
        page_size=args.szse_page_size,
        logger=logger,
    )

    # Date filter for SZSE at list level first
    szse_rows_filtered = []
    for r in szse_rows:
        ts = int(r.get("docpubtime") or 0)
        if ts <= 0:
            continue
        d = dt.datetime.fromtimestamp(ts / 1000).date()
        if start <= d <= end:
            szse_rows_filtered.append(r)
    logger.info("SZSE rows in date window: %s", len(szse_rows_filtered))

    # SSE detail candidates:
    # - only issuance announcements (1101)
    # - title includes rural keyword from query already; keep result-oriented titles first
    sse_candidates = []
    for r in sse_rows:
        publish_date = (r.get("sseDate") or "").split(" ")[0]
        if not in_date_window(publish_date, start, end):
            continue
        if str(r.get("orgBulletinType") or "") != "1101":
            continue
        sse_candidates.append(r)
    logger.info("SSE candidates (type=1101): %s", len(sse_candidates))

    logger.info("Step 3/5: Parse SSE details")
    sse_detail_rows: list[dict[str, Any]] = []
    with cf.ThreadPoolExecutor(max_workers=args.max_workers) as ex:
        futures = [ex.submit(parse_sse_detail, session, row, cache_pdf_dir) for row in sse_candidates]
        for fut in cf.as_completed(futures):
            sse_detail_rows.append(fut.result())
    logger.info("SSE parsed rows: %s", len(sse_detail_rows))

    # Prefer explicit result announcements; keep all for fallback if needed.
    sse_result_pref = []
    sse_non_result = []
    for r in sse_detail_rows:
        title = r.get("title") or ""
        if any(hint in title for hint in SSE_RESULT_TITLE_HINTS):
            sse_result_pref.append(r)
        else:
            sse_non_result.append(r)
    logger.info("SSE result-title rows: %s, other 1101 rows: %s", len(sse_result_pref), len(sse_non_result))

    logger.info("Step 4/5: Parse SZSE details")
    szse_detail_rows: list[dict[str, Any]] = []
    with cf.ThreadPoolExecutor(max_workers=args.max_workers) as ex:
        futures = [ex.submit(parse_szse_detail, session, row) for row in szse_rows_filtered]
        for fut in cf.as_completed(futures):
            szse_detail_rows.append(fut.result())
    logger.info("SZSE parsed rows: %s", len(szse_detail_rows))

    # Secondary tracing for SZSE listing notices -> result announcements
    szse_detail_rows = enhance_szse_rows_with_result_announcements(
        session=session,
        rows=szse_detail_rows,
        cache_pdf_dir=cache_szse_pdf_dir,
        max_workers=args.max_workers,
        logger=logger,
    )

    # Merge and clean
    raw_all = sse_result_pref + sse_non_result + szse_detail_rows
    df_raw = pd.DataFrame(raw_all)
    if df_raw.empty:
        logger.warning("No records extracted.")
        df_raw.to_csv(output_dir / "bond_detail_raw.csv", index=False, encoding="utf-8-sig")
        return

    # Keep records with amount and in target issue-result date window.
    df_raw["amount_billion"] = pd.to_numeric(df_raw["amount_billion"], errors="coerce")
    df_raw["issue_result_date"] = df_raw["issue_result_date"].astype(str)
    df_raw["publish_date"] = df_raw["publish_date"].astype(str)
    df_raw.to_csv(output_dir / "bond_detail_raw.csv", index=False, encoding="utf-8-sig")

    raw_rows = df_raw.to_dict(orient="records")
    deduped = dedupe_rows(raw_rows)
    df = pd.DataFrame(deduped)

    df["amount_billion"] = pd.to_numeric(df["amount_billion"], errors="coerce")
    df = df[df["amount_billion"].notna()].copy()
    df["issue_result_date_dt"] = pd.to_datetime(df["issue_result_date"], errors="coerce")
    df = df[df["issue_result_date_dt"].notna()].copy()
    df = df[
        (df["issue_result_date_dt"].dt.date >= start)
        & (df["issue_result_date_dt"].dt.date <= end)
    ].copy()
    df["issue_month"] = df["issue_result_date_dt"].dt.strftime("%Y-%m")
    df["security_code"] = df["security_code"].apply(normalize_security_code)
    df["security_abbr"] = df["security_abbr"].fillna("").astype(str).str.strip()
    df.sort_values(["issue_result_date_dt", "exchange", "security_code"], inplace=True)

    # Output cleaned details and monthly stats
    logger.info("Step 5/5: Export CSV files")
    out_cols = [
        "exchange",
        "security_code",
        "security_abbr",
        "bond_name",
        "issue_result_date",
        "issue_date_source",
        "amount_billion",
        "amount_source",
        "publish_date",
        "title",
        "url",
        "parse_note",
        "trace_result_url",
        "trace_result_title",
        "trace_result_publish_date",
    ]
    for col in out_cols:
        if col not in df.columns:
            df[col] = ""

    df[out_cols + ["issue_month"]].to_csv(
        output_dir / "bond_detail_clean.csv",
        index=False,
        encoding="utf-8-sig",
    )

    monthly_total, monthly_by_exchange = build_monthly_outputs(df)
    monthly_total.to_csv(output_dir / "monthly_total.csv", index=False, encoding="utf-8-sig")
    monthly_by_exchange.to_csv(output_dir / "monthly_by_exchange.csv", index=False, encoding="utf-8-sig")
    save_monthly_chart(monthly_total, output_dir / "monthly_total.png")

    logger.info("Done. clean_rows=%s, monthly_points=%s", len(df), len(monthly_total))
    logger.info("Output directory: %s", output_dir.resolve())


if __name__ == "__main__":
    run(parse_args())

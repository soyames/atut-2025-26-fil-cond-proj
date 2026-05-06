from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import date
import json
from pathlib import Path
import re
from typing import Iterable
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from src.etl.logging_utils import get_logger


LOGGER = get_logger(__name__)


@dataclass(frozen=True)
class WebBook:
    title: str
    price_gbp: float
    rating: str
    availability: str
    availability_count: int | None
    category: str
    upc: str
    product_type: str
    price_excl_tax_gbp: float
    price_incl_tax_gbp: float
    tax_gbp: float
    num_reviews: int
    description: str
    product_page_url: str
    source_page: str
    ingestion_date: str


@dataclass(frozen=True)
class ListingBook:
    title: str
    price_gbp: float
    rating: str
    availability: str
    detail_url: str
    source_page: str


def parse_rating(class_tokens: list[str]) -> str:
    ratings = {"One", "Two", "Three", "Four", "Five"}
    for token in class_tokens:
        if token in ratings:
            return token
    return "Unknown"


def parse_currency_gbp(raw: str, *, source: str) -> float:
    cleaned_price = re.sub(r"[^0-9.]", "", raw.strip())
    if not cleaned_price:
        raise ValueError(f"Invalid currency '{raw}' from {source}")
    return float(cleaned_price)


def parse_availability_count(raw: str) -> int | None:
    match = re.search(r"\((\d+)\s+available\)", raw)
    if not match:
        return None
    return int(match.group(1))


def parse_books_html(html: str, page_url: str) -> list[ListingBook]:
    soup = BeautifulSoup(html, "html.parser")
    rows: list[ListingBook] = []
    for article in soup.select("article.product_pod"):
        title = article.select_one("h3 a")["title"].strip()
        price_raw = article.select_one("p.price_color").text.strip()
        availability = article.select_one("p.instock.availability").text.strip()
        rating = parse_rating(article.select_one("p.star-rating").get("class", []))
        detail_rel = article.select_one("h3 a").get("href", "")
        detail_url = urljoin(page_url, detail_rel)
        rows.append(
            ListingBook(
                title=title,
                price_gbp=parse_currency_gbp(price_raw, source=page_url),
                rating=rating,
                availability=availability,
                detail_url=detail_url,
                source_page=page_url,
            )
        )
    return rows


def parse_product_table(soup: BeautifulSoup) -> dict[str, str]:
    values: dict[str, str] = {}
    for row in soup.select("table.table.table-striped tr"):
        key = row.select_one("th").get_text(strip=True)
        value = row.select_one("td").get_text(strip=True)
        values[key] = value
    return values


def parse_product_description(soup: BeautifulSoup) -> str:
    description_title = soup.select_one("#product_description")
    if description_title is None:
        return ""
    paragraph = description_title.find_next_sibling("p")
    if paragraph is None:
        return ""
    return paragraph.get_text(strip=True)


def parse_book_detail_html(listing: ListingBook, detail_html: str) -> WebBook:
    soup = BeautifulSoup(detail_html, "html.parser")
    values = parse_product_table(soup)
    breadcrumbs = [a.get_text(strip=True) for a in soup.select("ul.breadcrumb li a")]
    category = breadcrumbs[-1] if breadcrumbs else "Unknown"
    availability_detail = values.get("Availability", listing.availability)

    return WebBook(
        title=listing.title,
        price_gbp=listing.price_gbp,
        rating=listing.rating,
        availability=availability_detail,
        availability_count=parse_availability_count(availability_detail),
        category=category,
        upc=values.get("UPC", ""),
        product_type=values.get("Product Type", ""),
        price_excl_tax_gbp=parse_currency_gbp(values.get("Price (excl. tax)", "0"), source=listing.detail_url),
        price_incl_tax_gbp=parse_currency_gbp(values.get("Price (incl. tax)", "0"), source=listing.detail_url),
        tax_gbp=parse_currency_gbp(values.get("Tax", "0"), source=listing.detail_url),
        num_reviews=int(values.get("Number of reviews", "0")),
        description=parse_product_description(soup),
        product_page_url=listing.detail_url,
        source_page=listing.source_page,
        ingestion_date=str(date.today()),
    )


def iter_catalog_urls(base_url: str, max_pages: int) -> Iterable[str]:
    for idx in range(1, max_pages + 1):
        yield base_url.replace("page-1.html", f"page-{idx}.html")


def extract_books_web(base_url: str, max_pages: int, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"books_web_{date.today()}.jsonl"
    LOGGER.info("Starting web extraction")

    with output_path.open("w", encoding="utf-8") as file:
        for url in iter_catalog_urls(base_url, max_pages):
            response = requests.get(url, timeout=20)
            response.raise_for_status()
            listings = parse_books_html(response.text, url)
            for listing in listings:
                detail_response = requests.get(listing.detail_url, timeout=20)
                detail_response.raise_for_status()
                row = parse_book_detail_html(listing, detail_response.text)
                file.write(json.dumps(asdict(row), ensure_ascii=True) + "\n")
    LOGGER.info(f"Web extraction completed: {output_path}")
    return output_path


from src.etl.extract.books_web import (
    ListingBook,
    parse_availability_count,
    parse_book_detail_html,
    parse_books_html,
    parse_currency_gbp,
    parse_rating,
)


def test_parse_rating() -> None:
    assert parse_rating(["star-rating", "Three"]) == "Three"
    assert parse_rating(["star-rating"]) == "Unknown"


def test_parse_books_html() -> None:
    html = """
    <article class="product_pod">
      <h3><a href="../../../book-a/index.html" title="Book A"></a></h3>
      <p class="price_color">£21.10</p>
      <p class="instock availability">In stock</p>
      <p class="star-rating Four"></p>
    </article>
    """
    rows = parse_books_html(html, "http://example/page-1.html")
    assert len(rows) == 1
    assert rows[0].title == "Book A"
    assert rows[0].rating == "Four"
    assert rows[0].detail_url == "http://example/book-a/index.html"


def test_parse_currency_and_availability() -> None:
    assert parse_currency_gbp("Â51.77", source="x") == 51.77
    assert parse_availability_count("In stock (22 available)") == 22
    assert parse_availability_count("In stock") is None


def test_parse_book_detail_html() -> None:
    listing = ListingBook(
        title="Book A",
        price_gbp=21.1,
        rating="Four",
        availability="In stock",
        detail_url="http://example/book-a/index.html",
        source_page="http://example/page-1.html",
    )
    detail_html = """
    <ul class="breadcrumb">
      <li><a>Home</a></li>
      <li><a>Books</a></li>
      <li><a>Poetry</a></li>
      <li class="active">Book A</li>
    </ul>
    <div id="product_description"></div>
    <p>Some description.</p>
    <table class="table table-striped">
      <tr><th>UPC</th><td>abc</td></tr>
      <tr><th>Product Type</th><td>Books</td></tr>
      <tr><th>Price (excl. tax)</th><td>£21.10</td></tr>
      <tr><th>Price (incl. tax)</th><td>£22.10</td></tr>
      <tr><th>Tax</th><td>£1.00</td></tr>
      <tr><th>Availability</th><td>In stock (22 available)</td></tr>
      <tr><th>Number of reviews</th><td>7</td></tr>
    </table>
    """
    row = parse_book_detail_html(listing, detail_html)
    assert row.category == "Poetry"
    assert row.upc == "abc"
    assert row.availability_count == 22
    assert row.num_reviews == 7


def test_parse_book_detail_html_without_description() -> None:
    listing = ListingBook(
        title="Book B",
        price_gbp=10.0,
        rating="Three",
        availability="In stock",
        detail_url="http://example/book-b/index.html",
        source_page="http://example/page-1.html",
    )
    detail_html = """
    <ul class="breadcrumb"><li><a>Home</a></li><li><a>Books</a></li><li><a>Mystery</a></li></ul>
    <table class="table table-striped">
      <tr><th>UPC</th><td>u2</td></tr>
      <tr><th>Product Type</th><td>Books</td></tr>
      <tr><th>Price (excl. tax)</th><td>£10.00</td></tr>
      <tr><th>Price (incl. tax)</th><td>£10.00</td></tr>
      <tr><th>Tax</th><td>£0.00</td></tr>
      <tr><th>Availability</th><td>In stock</td></tr>
      <tr><th>Number of reviews</th><td>0</td></tr>
    </table>
    """
    row = parse_book_detail_html(listing, detail_html)
    assert row.description == ""


def test_parse_book_detail_html_description_title_without_paragraph() -> None:
    listing = ListingBook(
        title="Book C",
        price_gbp=10.0,
        rating="Three",
        availability="In stock",
        detail_url="http://example/book-c/index.html",
        source_page="http://example/page-1.html",
    )
    detail_html = """
    <div id="product_description"></div>
    <table class="table table-striped">
      <tr><th>UPC</th><td>u3</td></tr>
      <tr><th>Product Type</th><td>Books</td></tr>
      <tr><th>Price (excl. tax)</th><td>£10.00</td></tr>
      <tr><th>Price (incl. tax)</th><td>£10.00</td></tr>
      <tr><th>Tax</th><td>£0.00</td></tr>
      <tr><th>Availability</th><td>In stock</td></tr>
      <tr><th>Number of reviews</th><td>0</td></tr>
    </table>
    """
    row = parse_book_detail_html(listing, detail_html)
    assert row.description == ""


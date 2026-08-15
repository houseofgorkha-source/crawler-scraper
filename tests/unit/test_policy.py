from crawler.policy import (
    DEFAULT_CRAWL_DELAY_MS, MAX_CRAWL_DELAY_MS, MIN_CRAWL_DELAY_MS, parse_robots,
)


def test_2xx_obeys_disallow_rules():
    body = "User-agent: *\nDisallow: /private\n"
    policy = parse_robots("example.com", body, 200)
    assert policy.is_crawlable is True
    assert policy.check_allowed("https://example.com/private/page") is False
    assert policy.check_allowed("https://example.com/public") is True


def test_404_means_no_restrictions():
    policy = parse_robots("example.com", None, 404)
    assert policy.is_crawlable is True
    assert policy.check_allowed("https://example.com/anything") is True


def test_410_means_no_restrictions():
    policy = parse_robots("example.com", None, 410)
    assert policy.is_crawlable is True


def test_401_disallows_whole_host():
    policy = parse_robots("example.com", None, 401)
    assert policy.is_crawlable is False
    assert policy.check_allowed("https://example.com/anything") is False


def test_403_disallows_whole_host():
    policy = parse_robots("example.com", None, 403)
    assert policy.is_crawlable is False


def test_5xx_disallows_for_now():
    policy = parse_robots("example.com", None, 503)
    assert policy.is_crawlable is False
    # 5xx also leaves fetched_at unset, so the policy reads as stale and
    # gets re-resolved rather than being cached as a durable failure.
    assert policy.is_stale is True


def test_unresolved_policy_fails_closed():
    # No robots ever fetched: DomainPolicy with fetched_at=None, no parser.
    from crawler.policy import DomainPolicy
    policy = DomainPolicy(host="example.com", is_crawlable=True,
                          crawl_delay_ms=1000, fetched_at=None)
    assert policy.check_allowed("https://example.com/anything") is False


def test_crawl_delay_declared_is_honored():
    body = "User-agent: *\nCrawl-delay: 5\n"
    policy = parse_robots("example.com", body, 200)
    assert policy.crawl_delay_ms == 5000


def test_crawl_delay_floor_enforced():
    # Python's stdlib RobotFileParser.crawl_delay() only recognizes integer
    # values (a fractional "Crawl-delay: 0.05" silently parses as None), so
    # the floor is exercised via Request-rate instead: 20 requests/second
    # implies 50ms between requests, well under the floor.
    body = "User-agent: *\nRequest-rate: 20/1\n"
    policy = parse_robots("example.com", body, 200)
    assert policy.crawl_delay_ms == MIN_CRAWL_DELAY_MS


def test_crawl_delay_ceiling_enforced():
    body = "User-agent: *\nCrawl-delay: 999999\n"
    policy = parse_robots("example.com", body, 200)
    assert policy.crawl_delay_ms == MAX_CRAWL_DELAY_MS


def test_no_crawl_delay_directive_uses_default():
    body = "User-agent: *\nDisallow:\n"
    policy = parse_robots("example.com", body, 200)
    assert policy.crawl_delay_ms == DEFAULT_CRAWL_DELAY_MS


def test_empty_body_treated_like_not_found():
    policy = parse_robots("example.com", "", 200)
    assert policy.is_crawlable is True
    assert policy.check_allowed("https://example.com/anything") is True

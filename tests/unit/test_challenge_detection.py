from crawler.fetch import detect_challenge


def test_captcha_is_detected():
    assert detect_challenge(b"<html>please complete CAPTCHA</html>") == "captcha"


def test_recaptcha_is_detected():
    assert detect_challenge(b"recaptcha verification required") == "recaptcha"


def test_cloudflare_challenge_is_detected():
    assert detect_challenge(b"<title>Just a Moment...</title>") == "cloudflare_challenge"


def test_human_verification_is_detected():
    assert detect_challenge(b"Please verify you are human") == "human_verification"


def test_normal_page_is_not_detected():
    assert detect_challenge(
        b"<html><body>Amazon products and prices</body></html>"
    ) is None


def test_empty_body_is_not_detected():
    assert detect_challenge(None) is None

import pytest

from polycollect import gamma


@pytest.mark.parametrize("text, expected", [
    ("fed-decision-in-december", "fed-decision-in-december"),
    ("market:will-the-fed-cut", "market:will-the-fed-cut"),
    ("https://polymarket.com/event/fed-decision-in-december", "event:fed-decision-in-december"),
    ("https://polymarket.com/event/fed-decision-in-december/will-the-fed-cut?tid=1", "market:will-the-fed-cut"),
    ("polymarket.com/market/will-the-fed-cut/", "market:will-the-fed-cut"),
])
def test_slug_from_input(text, expected):
    assert gamma.slug_from_input(text) == expected


def test_slug_from_input_rejects_other_urls():
    with pytest.raises(ValueError):
        gamma.slug_from_input("https://polymarket.com/sports/live")


def market(slug, ids, outcomes=("Yes", "No"), closed=False):
    return {"slug": slug, "conditionId": f"0x{slug}", "closed": closed,
            "clobTokenIds": str(list(ids)).replace("'", '"'), "outcomes": str(list(outcomes)).replace("'", '"')}


@pytest.fixture
def api(monkeypatch):
    """Fake Gamma: {path: {slug: [results]}}; records every lookup."""
    data = {"/events": {}, "/markets": {}}
    calls = []

    def fake_get(path, params):
        calls.append((path, params["slug"]))
        return data[path].get(params["slug"], [])

    monkeypatch.setattr(gamma, "_get", fake_get)
    return data, calls


def test_event_resolves_to_its_open_markets(api):
    data, _ = api
    data["/events"]["election"] = [{"markets": [market("a", ["1", "2"]), market("b", ["3", "4"], closed=True)]}]
    assert [(t.slug, t.outcome, t.asset_id, t.market) for t in gamma.resolve("election")] == [
        ("a", "Yes", "1", "0xa"), ("a", "No", "2", "0xa"),
    ]


def test_bare_slug_falls_back_to_market(api):
    data, calls = api
    data["/markets"]["a"] = [market("a", ["1", "2"])]
    assert [t.asset_id for t in gamma.resolve("a")] == ["1", "2"]
    assert calls == [("/events", "a"), ("/markets", "a")]


def test_market_prefix_skips_an_event_with_the_same_slug(api):
    data, calls = api
    data["/events"]["main"] = [{"markets": [market("main", ["1", "2"]), market("side", ["3", "4"])]}]
    data["/markets"]["main"] = [market("main", ["1", "2"])]
    assert [t.asset_id for t in gamma.resolve("market:main")] == ["1", "2"]
    assert calls == [("/markets", "main")]


def test_closed_market_resolves_to_nothing_and_missing_slug_raises(api):
    data, _ = api
    data["/markets"]["done"] = [market("done", ["1", "2"], closed=True)]
    assert gamma.resolve("done") == []
    with pytest.raises(gamma.GammaError):
        gamma.resolve("nope")
    with pytest.raises(gamma.GammaError):
        gamma.resolve("typo:nope")

from test.test_multivariate_comparison import e2e


def test_e2e(monkeypatch):
    e2e(monkeypatch, test_id='r2_exactly_1')

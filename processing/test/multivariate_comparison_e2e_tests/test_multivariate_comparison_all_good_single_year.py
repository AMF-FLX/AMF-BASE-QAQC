from test.test_multivariate_comparison import e2e


def test_e2e(monkeypatch):
    e2e(monkeypatch, test_id='all_good_single_year')

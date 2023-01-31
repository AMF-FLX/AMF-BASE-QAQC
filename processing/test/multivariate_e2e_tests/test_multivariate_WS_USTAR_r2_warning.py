from test.test_multivariate_intercomparison import e2e


def test_e2e(monkeypatch):
    e2e(monkeypatch, test_id='WS_USTAR_r2_warning')

from runpod_pool.autoscale import Autoscaler, recommend

NOW = 1_000_000.0


def test_recommend_scales_with_the_ratio_like_an_hpa():
    assert recommend(ready=2, value=6, target=3, lo=2, hi=4) == 4
    assert recommend(ready=2, value=4.5, target=3, lo=2, hi=4) == 3
    assert recommend(ready=3, value=0.5, target=3, lo=2, hi=4) == 2   # clamped to min
    assert recommend(ready=2, value=30, target=3, lo=2, hi=4) == 4    # clamped to max


def test_recommend_has_a_tolerance_and_needs_data():
    assert recommend(ready=2, value=3.2, target=3, lo=2, hi=4) == 2   # within 10%: stay
    assert recommend(ready=0, value=9, target=3, lo=2, hi=4) is None  # nothing to scale from
    assert recommend(ready=2, value=None, target=3, lo=2, hi=4) is None


def scaler(**kw):
    return Autoscaler(up_window=120, down_window=1800, **kw)


def test_scale_up_needs_the_recommendation_sustained():
    a = scaler()
    assert a.step(prev=2, rec=3, now=NOW) == 2          # first sample: not yet sustained
    assert a.step(prev=2, rec=3, now=NOW + 60) == 2
    assert a.step(prev=2, rec=3, now=NOW + 120) == 3    # 2 min above: up
    a = scaler()
    for t, rec in ((0, 3), (60, 2), (120, 3)):          # a dip resets it
        assert a.step(prev=2, rec=rec, now=NOW + t) == 2


def test_scale_up_takes_the_lowest_recommendation_in_the_window():
    a = scaler()
    for t, rec in ((0, 4), (60, 3), (120, 4)):
        out = a.step(prev=2, rec=rec, now=NOW + t)
    assert out == 3


def test_scale_down_uses_the_highest_recommendation_in_the_window():
    a = scaler()
    out = None
    for t in range(0, 1801, 60):
        out = a.step(prev=4, rec=3 if t == 600 else 2, now=NOW + t)
    assert out == 3                                     # the 3 at t=600 is still in the window
    for t in range(1860, 2500, 60):
        out = a.step(prev=3, rec=2, now=NOW + t)
    assert out == 2


def test_no_recommendation_keeps_the_current_size():
    a = scaler()
    assert a.step(prev=3, rec=None, now=NOW) == 3


def test_state_round_trips():
    a = scaler()
    a.step(prev=2, rec=3, now=NOW)
    b = Autoscaler.from_state(a.state(), up_window=120, down_window=1800)
    assert b.step(prev=2, rec=3, now=NOW + 120) == 3


def test_jittered_loops_still_scale_down():
    a = scaler()
    t, out = 0.0, None
    while t < 2400:
        out = a.step(prev=3, rec=2, now=NOW + t)
        t += 31.7  # real loops drift off the window edges
    assert out == 2
    assert len(a.recs) <= 1800 / 31.7 + 3  # bounded

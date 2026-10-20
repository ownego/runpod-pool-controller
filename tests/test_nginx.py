import pytest

from runpod_pool.nginx import parse_slots, slot_letter

def test_parse_slots():
    conf = """
    server {
      listen 80;
      location / { proxy_pass http://runpod_removebg; }
    }
    server {
      listen 8081;
      set $upstream_host aaaaaaaaaaaaaa-80.proxy.runpod.net;
    }
    server {
      listen 8082;
      set $upstream_host bbbbbbbbbbbbbb-80.proxy.runpod.net;
    }
    """
    assert parse_slots(conf) == {8081: "aaaaaaaaaaaaaa", 8082: "bbbbbbbbbbbbbb"}


def test_slot_letter():
    assert slot_letter(8081, 8081) == "a"
    assert slot_letter(8082, 8081) == "b"
    with pytest.raises(ValueError):
        slot_letter(8080, 8081)


def test_assign_ports_keeps_existing_ports():
    from runpod_pool.nginx import assign_ports
    current = {8081: "a" * 14, 8082: "b" * 14}
    assert assign_ports(current, ["b" * 14, "c" * 14], 8081, 4) == {8081: "c" * 14, 8082: "b" * 14}
    with pytest.raises(ValueError):
        assign_ports({}, ["a", "b", "c"], 8081, 2)


def test_render_round_trips_and_is_valid_nginx(tmp_path):
    from runpod_pool.nginx import render, SLOTS_KEY, UPSTREAM_KEY, METRICS_PIN_KEY
    slots = {8081: "a" * 14, 8083: "c" * 14}
    data = render("remove-bg", slots, 8081, {"a" * 14: "RTX 4090, US-NC-1"})
    assert parse_slots(data[SLOTS_KEY]) == slots
    assert "server 127.0.0.1:8083 max_fails=3 fail_timeout=30s;" in data[UPSTREAM_KEY]
    assert "proxy_pass http://127.0.0.1:8081;" in data[METRICS_PIN_KEY]
    assert "proxy_read_timeout 120s;" in data[SLOTS_KEY]
    empty = render("remove-bg", {}, 8081)
    assert parse_slots(empty[SLOTS_KEY]) == {} and "down;" in empty[UPSTREAM_KEY]


def test_render_uses_the_pods_port_and_parses_it_back():
    from runpod_pool.nginx import SLOTS_KEY, render
    conf = render("p", {8081: "aaaaaaaaaaaaaa"}, 8081, pod_port=7860)[SLOTS_KEY]
    assert "aaaaaaaaaaaaaa-7860.proxy.runpod.net" in conf
    assert parse_slots(conf) == {8081: "aaaaaaaaaaaaaa"}

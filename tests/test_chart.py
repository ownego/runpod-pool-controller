"""The Helm chart must stay in step with the code and deploy/."""

from pathlib import Path

from runpod_pool import nginx
from runpod_pool.kube import GROUP

ROOT = Path(__file__).resolve().parents[1]
CHART = ROOT / "charts/runpod-pool-controller"


def test_chart_crd_is_deploy_crd():
    assert (CHART / "crds/runpodpools.yaml").read_text() == (ROOT / "deploy/crd.yaml").read_text()


def test_chart_app_version_is_the_package_version():
    import runpod_pool
    assert f'appVersion: "{runpod_pool.__version__}"' in (CHART / "Chart.yaml").read_text()


def test_chart_slots_bootstrap_matches_the_empty_render():
    # The template builds the empty render with printf; check its pieces
    # against nginx.render() so a format change there breaks this test.
    tpl = (CHART / "templates/slots-configmaps.yaml").read_text()
    data = nginx.render("POOL", {}, 9001)
    header = nginx._HEADER.format(pool="POOL")
    assert header.replace("POOL", "%s").replace("\n", "\\n") in tpl
    for key, body in data.items():
        rest = body[len(header):].replace("9001", "%d").replace("\n", "\\n")
        assert f'{key}: {{{{ printf "%s{rest}"' in tpl, key


def test_chart_default_group():
    assert f'default "{GROUP}"' in (CHART / "templates/_helpers.tpl").read_text()

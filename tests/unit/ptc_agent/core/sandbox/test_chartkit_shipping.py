"""chartkit (libs/chartkit) ships into every sandbox as an internal package,
with the render page its snapshots load."""

from __future__ import annotations

from ptc_agent.core.sandbox import _shared


def _shipped(tmp_path) -> set[str]:
    src = tmp_path / "src"
    src.mkdir()
    (src / "__init__.py").write_text("")
    return {str(rel) for _, rel in _shared._internal_package_files(src, livefs=False)}


def test_chartkit_ships_as_an_internal_package(tmp_path):
    assert {"chartkit/__init__.py", "chartkit/chart.py", "chartkit/ta.py", "chartkit/analysis.py",
            "chartkit/data.py", "chartkit/schema.py", "chartkit/protocol.py", "chartkit/_util.py",
            "chartkit/_snapshot.py", "chartkit/_view.py", "chartkit/_bars_source.py",
            "chartkit/_renderer.py", "chartkit/_report.py"} <= _shipped(tmp_path)


def test_render_page_ships_with_chartkit(tmp_path, monkeypatch):
    pkg = tmp_path / "chartkit"
    (pkg / "render" / "assets").mkdir(parents=True)
    (pkg / "__init__.py").write_text("")
    (pkg / "render" / "chart-render.html").write_text("<html></html>")
    (pkg / "render" / "assets" / "chart-render-abc.js").write_text("")
    real = _shared._internal_package_dir
    monkeypatch.setattr(
        _shared, "_internal_package_dir", lambda m: pkg if m == "chartkit" else real(m)
    )
    assert {"chartkit/render/chart-render.html",
            "chartkit/render/assets/chart-render-abc.js"} <= _shipped(tmp_path)


def _chartkit_at(tmp_path, monkeypatch, *, render: bool):
    pkg = tmp_path / "chartkit"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("")
    if render:
        (pkg / "render").mkdir()
        (pkg / "render" / "chart-render.html").write_text("host")
    real = _shared._internal_package_dir
    monkeypatch.setattr(
        _shared, "_internal_package_dir", lambda m: pkg if m == "chartkit" else real(m)
    )


def _image_render(tmp_path, monkeypatch):
    image = tmp_path / "image-render"
    (image / "assets").mkdir(parents=True)
    (image / "chart-render.html").write_text("image")
    (image / "assets" / "chart-render-img.js").write_text("")
    monkeypatch.setattr(_shared, "_CHART_RENDER_FALLBACK", str(image))


def test_a_mounted_chartkit_without_a_render_page_ships_the_images(tmp_path, monkeypatch):
    # The dev stack mounts libs/chartkit over the image's copy; a checkout that
    # never built the page must still get snapshots.
    _chartkit_at(tmp_path, monkeypatch, render=False)
    _image_render(tmp_path, monkeypatch)
    src = tmp_path / "src"
    src.mkdir()
    (src / "__init__.py").write_text("")
    shipped = dict((str(rel), local) for local, rel in _shared._internal_package_files(src, livefs=False))
    assert shipped["chartkit/render/chart-render.html"].read_text() == "image"
    assert "chartkit/render/assets/chart-render-img.js" in shipped


def test_a_host_built_render_page_wins_over_the_images(tmp_path, monkeypatch):
    _chartkit_at(tmp_path, monkeypatch, render=True)
    _image_render(tmp_path, monkeypatch)
    src = tmp_path / "src"
    src.mkdir()
    (src / "__init__.py").write_text("")
    shipped = dict((str(rel), local) for local, rel in _shared._internal_package_files(src, livefs=False))
    assert shipped["chartkit/render/chart-render.html"].read_text() == "host"
    assert "chartkit/render/assets/chart-render-img.js" not in shipped

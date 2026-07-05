from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def _load_plugin_module():
    path = Path(__file__).parents[1] / "plugin.py"
    spec = importlib.util.spec_from_file_location(
        "test_feishu_plugin",
        path,
        submodule_search_locations=[str(path.parent)],
    )
    if spec is None or spec.loader is None:
        raise ImportError(str(path))
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


module = _load_plugin_module()
FeishuConfigModel = module.FeishuConfigModel
FeishuPlugin = module.FeishuPlugin


def test_feishu_plugin_without_config_returns_no_channels() -> None:
    plugin = FeishuPlugin()
    plugin.context = type("Ctx", (), {"config": None})()
    assert plugin.channels() == []


def test_feishu_plugin_with_config_returns_channel() -> None:
    plugin = FeishuPlugin()
    plugin.context = type(
        "Ctx",
        (),
        {
            "config": FeishuConfigModel(
                app_id="app",
                app_secret="secret",
                allow_from=[],
                domain="https://open.feishu.cn",
            )
        },
    )()
    assert len(plugin.channels()) == 1

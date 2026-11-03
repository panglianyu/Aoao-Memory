import os
import re
import yaml
from typing import Dict, Any


def _expand_env_vars(value: str) -> str:
    """解析字符串中的 ${VAR_NAME} 环境变量引用。"""
    if not isinstance(value, str):
        return value
    pattern = re.compile(r'\$\{([^}]+)\}')
    matches = pattern.findall(value)
    for var_name in matches:
        env_val = os.environ.get(var_name, "")
        value = value.replace(f"${{{var_name}}}", env_val)
    return value


def load_api_config() -> Dict[str, Any]:
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    path = os.path.join(base_dir, "config", "api.yaml")
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            raw = yaml.safe_load(f).get("openai", {})
        return {k: _expand_env_vars(v) for k, v in raw.items()}
    return {}
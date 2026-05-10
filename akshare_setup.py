"""AkShare 全局请求头与 urllib3 告警，减轻数据源反爬拦截。"""

_configured = False


def configure_akshare_http():
    global _configured
    if _configured:
        return
    import requests

    requests.packages.urllib3.disable_warnings()
    try:
        import akshare as ak
    except ImportError:
        _configured = True
        return
    # 旧版 AkShare 用包级 _global_dict；新版本可能已移除，勿访问以免 AttributeError
    gd = getattr(ak, '_global_dict', None)
    if isinstance(gd, dict):
        gd['headers'] = {
            'User-Agent': (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/120.0.0.0 Safari/537.36'
            )
        }
    _configured = True

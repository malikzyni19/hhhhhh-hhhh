from main import app


def register():
    if getattr(app, "_firebase_live_monitor_registered", False):
        return

    app._firebase_live_monitor_registered = True

    @app.after_request
    def load_firebase_live_monitor(response):
        if response.status_code != 200 or response.direct_passthrough:
            return response

        if "text/html" not in response.headers.get("Content-Type", ""):
            return response

        try:
            html = response.get_data(as_text=True)
        except Exception:
            return response

        marker = '<script type="module" src="/static/js/firebase-live-monitor.js"></script>'
        is_screener = "ZyNi Screener" in html and 'data-view="live"' in html
        if not is_screener or marker in html or "</body>" not in html:
            return response

        response.set_data(html.replace("</body>", marker + "\n</body>", 1))
        response.headers["Content-Length"] = str(len(response.get_data()))
        return response

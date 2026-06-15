def post_worker_init(worker):
    import firebase_live_monitor_bootstrap
    firebase_live_monitor_bootstrap.register()

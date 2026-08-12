"""Offline research studies.

Nothing in here is imported at app boot. main.py's admin debug route imports
cvd_flow_study lazily, inside the request handler, so a broken or missing
study file can never stop the site from starting.
"""

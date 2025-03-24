import datetime
import pathlib

def getmtime(f: pathlib.Path):
    return datetime.datetime.fromtimestamp(f.stat().st_mtime)


def getatime(f: pathlib.Path):
    return datetime.datetime.fromtimestamp(f.stat().st_atime)


def getctime(f: pathlib.Path):
    return datetime.datetime.fromtimestamp(f.stat().st_ctime)

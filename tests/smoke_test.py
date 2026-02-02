from intel.normalize import norm_text, split_asset_aliases

def test_norm_text():
    assert norm_text("DARZALEX (daratumumab)") == "darzalex daratumumab"

def test_split_asset_aliases():
    canonical, aliases = split_asset_aliases("JNJ-1900 (NBTXR3)")
    assert canonical == "JNJ-1900"
    assert any(a.upper() == "NBTXR3" for a in aliases)

if __name__ == "__main__":
    test_norm_text()
    test_split_asset_aliases()
    print("OK")

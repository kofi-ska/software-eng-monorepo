import os, sys, pprint

print("CWD:", os.getcwd())
print("FILE DIR:", os.path.dirname(__file__))
print("sys.path (first 5):")
pprint.pprint(sys.path[:5])

# manual fix test
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
print("ROOT_DIR:", ROOT_DIR)
sys.path.insert(0, ROOT_DIR)

try:
    import logic.core.Manager as mgr

    print("✅ import worked:", mgr)
except Exception as e:
    print("❌ import failed:", e)

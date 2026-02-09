# -------------------------------
# Run Feature Store Tests
# -------------------------------
# Runs feature store sanity check tests with Delta format

from tests.test_feature_store import run_all_feature_store_tests

if __name__ == "__main__":
    run_all_feature_store_tests()

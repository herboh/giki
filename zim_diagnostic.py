#!/usr/bin/env python3
"""
Simple diagnostic script to debug ZIM file reading issues.
"""

import sys
from pathlib import Path

try:
    from libzim.reader import Archive
except ImportError:
    print("ERROR: libzim not installed. Run: pip install libzim")
    sys.exit(1)


def diagnose_zim(zim_path):
    """Diagnose ZIM file structure and entry reading."""
    print(f"=== ZIM File Diagnostic ===")
    print(f"File: {zim_path}")

    try:
        archive = Archive(str(zim_path))
        print(f"✓ Successfully opened ZIM file")
        print(f"✓ Total entries: {archive.entry_count:,}")

        # Test different ways to read entries
        print(f"\n--- Testing Entry Access Methods ---")

        # Method 1: By ID
        print("Testing entry access by ID...")
        sample_ids = (
            [0, 1, 2, 100, 1000]
            if archive.entry_count > 1000
            else list(range(min(5, archive.entry_count)))
        )

        for entry_id in sample_ids:
            try:
                entry = archive.get_entry_by_id(entry_id)
                print(
                    f"  ID {entry_id}: path='{entry.path}', title='{entry.title}', is_redirect={entry.is_redirect}"
                )

                # Try to access content for non-redirects
                if not entry.is_redirect:
                    try:
                        content = bytes(entry.get_item().content)
                        print(f"    Content size: {len(content)} bytes")
                    except Exception as e:
                        print(f"    Content access failed: {e}")

            except Exception as e:
                print(f"  ID {entry_id}: ERROR - {e}")

        # Method 2: Iterate through entries
        print(f"\n--- Entry Iteration Test ---")
        print("First 10 entries via iteration:")

        try:
            count = 0
            for entry in archive:
                if count >= 10:
                    break
                print(
                    f"  {count}: path='{entry.path}', title='{entry.title}', namespace='{entry.path.split('/')[0] if '/' in entry.path else 'ROOT'}'"
                )
                count += 1

            if count == 0:
                print("  WARNING: No entries found via iteration!")

        except Exception as e:
            print(f"  ERROR during iteration: {e}")

        # Method 3: Namespace analysis
        print(f"\n--- Namespace Analysis ---")
        namespace_counts = {}
        sample_paths = []

        # Sample more entries to understand structure
        sample_size = min(1000, archive.entry_count)
        print(f"Analyzing first {sample_size} entries...")

        for i in range(sample_size):
            try:
                entry = archive.get_entry_by_id(i)
                path = entry.path

                if path:  # Only count non-empty paths
                    sample_paths.append(path)

                    # Extract namespace
                    if "/" in path:
                        namespace = path.split("/")[0]
                    else:
                        namespace = "ROOT"

                    namespace_counts[namespace] = namespace_counts.get(namespace, 0) + 1

            except Exception as e:
                print(f"    Error at entry {i}: {e}")
                continue

        print(f"Namespace distribution: {dict(sorted(namespace_counts.items()))}")
        print(f"Sample paths (first 20): {sample_paths[:20]}")

        # Method 4: Look for articles specifically
        print(f"\n--- Article Detection ---")
        article_namespaces = ["A", "C"]  # Old and new formats

        for ns in article_namespaces:
            print(f"Looking for {ns}/ entries...")
            found_count = 0
            sample_articles = []

            for i in range(min(5000, archive.entry_count)):
                try:
                    entry = archive.get_entry_by_id(i)
                    if entry.path.startswith(f"{ns}/"):
                        found_count += 1
                        if len(sample_articles) < 10:
                            sample_articles.append(entry.path)
                except:
                    continue

            print(f"  Found {found_count} entries in {ns}/ namespace")
            if sample_articles:
                print(f"  Sample {ns}/ entries: {sample_articles[:5]}")

        # Method 5: Test title matching
        print(f"\n--- Title Matching Test ---")
        test_titles = ["Gamma_Cygni", "Gamma_Data", "Main_Page"]

        for title in test_titles:
            try:
                # Try direct lookup
                entry = archive.get_entry_by_path(f"A/{title}")
                print(f"  ✓ Found A/{title}")
            except:
                try:
                    entry = archive.get_entry_by_path(f"C/{title}")
                    print(f"  ✓ Found C/{title}")
                except:
                    print(f"  ✗ Not found: {title}")

        return True

    except Exception as e:
        print(f"✗ Failed to open ZIM file: {e}")
        return False


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python zim_diagnostic.py <zim_file>")
        sys.exit(1)

    zim_path = Path(sys.argv[1])
    if not zim_path.exists():
        print(f"ERROR: File not found: {zim_path}")
        sys.exit(1)

    success = diagnose_zim(zim_path)
    sys.exit(0 if success else 1)

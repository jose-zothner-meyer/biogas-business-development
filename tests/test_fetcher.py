#!/usr/bin/env python3
"""Test the updated MaStR fetcher."""

from mastr_fetcher import MaStrFetcher

def main():
    print("🧪 Testing updated MaStR fetcher...")
    
    # Initialize fetcher  
    fetcher = MaStrFetcher()
    
    # Test the fetch_all method with manual export
    print("\n📤 Testing fetch_all method...")
    try:
        paths = fetcher.fetch_all(method="bulk", export_csv=True)
        
        print(f"\n✅ fetch_all completed! Returned paths:")
        for key, path in paths.items():
            size = path.stat().st_size if path.exists() else 0
            print(f"   {key}: {path} ({size:,} bytes)")
            
        return paths
        
    except Exception as e:
        print(f"❌ fetch_all failed: {e}")
        import traceback
        traceback.print_exc()
        return {}

if __name__ == "__main__":
    main()

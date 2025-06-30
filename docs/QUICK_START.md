# German Biogas Database - Quick Start Guide

## 🚀 Getting Started with Organized Structure

### 1. **Main Operations**
```bash
# Run complete data pipeline
python main.py

# Run enhanced analysis and processing  
python run_analysis.py

# View large CSV files
python view_csv.py head 20
python view_csv.py search biogas
```

### 2. **Project Structure**
- **`src/core/`** - Core data processing modules
- **`src/analysis/`** - Business analysis and enhancement
- **`data/processed/`** - Clean, ready-to-use datasets
- **`data/raw/`** - Raw MaStR data cache
- **`tools/`** - Utility scripts and helpers
- **`tests/`** - Testing and debugging
- **`config/`** - Configuration files

### 3. **Key Data Files**
- **`data/processed/german_biogas_plants_enhanced.csv`** - PRIMARY dataset (23,984 plants + operator details)
- **`data/processed/german_biogas_all_operators_deduplicated.csv`** - Complete operator database (4.7M records)
- **`data/processed/german_biogas_plants_with_contacts_clean.csv`** - Cleaned original plants

### 4. **Development**
```bash
# Run tests
python tests/test_fetcher.py

# Debug data issues
python tests/debug_db.py

# Direct tool usage
python tools/csv_viewer.py info
```

### 5. **Business Analysis**
```python
import pandas as pd

# Load enhanced plant data
plants = pd.read_csv("data/processed/german_biogas_plants_enhanced.csv")

# Analyze by capacity
capacity_analysis = plants.groupby('postal_code')['capacity_el_kW'].sum().sort_values(ascending=False)
```

---
**Clean, organized, and business-ready!** 🌱⚡

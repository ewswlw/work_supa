# Accessing Project on Different Computers

## 💻 **Two Methods to Access Work_Supa Project**

### **Method 1: Dropbox Sync (Recommended for Development)**

#### **How to Access:**
1. **Install Dropbox** on the new computer
2. **Sign in** with your Dropbox account
3. **Wait for sync** to complete
4. **Navigate to**: `YTM Capital Dropbox/Eddy Winiarz/Trading/COF/Models/Unfinished Models/Eddy/Python Projects/work_supa`
5. **Open in VS Code** or your preferred editor

#### **What You Get:**
- ✅ **Complete working environment**
- ✅ **All source code** (`.py`, `.yaml`, `.md` files)
- ✅ **All raw data** (Excel files, parquet files)
- ✅ **All processed data** (pipeline outputs)
- ✅ **All logs and cache** (runtime history)
- ✅ **Configuration files** (ready to run)
- ✅ **Documentation** (all notes and guides)

#### **Pros:**
- **Instant setup** - everything ready to go
- **No git knowledge** required
- **Complete data** - all raw and processed files
- **Runtime history** preserved
- **Same environment** as original computer

#### **Cons:**
- **Larger file size** (includes temporary files)
- **Requires Dropbox** subscription/storage
- **May include unnecessary** cache files

---

### **Method 2: GitHub Clone (Clean Repository)**

#### **How to Access:**
1. **Install Git** on the new computer
2. **Open terminal/command prompt**
3. **Clone repository**:
   ```bash
   git clone https://github.com/ewswlw/work_supa.git
   cd work_supa
   ```
4. **Install Poetry** (if not already installed)
5. **Install dependencies**:
   ```bash
   poetry install
   ```
6. **Run pipeline** to generate data

#### **What You Get:**
- ✅ **Clean source code** only
- ✅ **Documentation** and configuration
- ✅ **Project structure**
- ❌ **No raw data** (need to add manually)
- ❌ **No processed data** (need to run pipeline)
- ❌ **No logs/cache** (will be generated fresh)

#### **Pros:**
- **Clean environment** - no clutter
- **Fast download** - small repository size
- **Professional setup** - only source code
- **Version controlled** - proper git history
- **Collaboration ready** - easy to share

#### **Cons:**
- **Setup required** - need to install dependencies
- **Missing data** - need raw files separately
- **Git knowledge** helpful
- **Pipeline run** required to generate processed data

---

## 🎯 **Recommendations by Use Case**

### **For Personal Development Work:**
**Use Dropbox Method**
- You want to continue exactly where you left off
- All your data and settings preserved
- Instant productivity

### **For Fresh Setup or Collaboration:**
**Use GitHub Method**
- Clean professional environment
- Easy to share with others
- Version controlled properly

### **For Data Analysis:**
**Use Dropbox Method**
- All processed data immediately available
- No need to regenerate large datasets
- Runtime logs preserved for reference

### **For Code Review or Testing:**
**Use GitHub Method**
- Clean code without clutter
- Test pipeline from scratch
- Verify setup instructions work

---

## 📁 **File Locations Guide**

### **Dropbox Path:**
```
C:\Users\[Username]\YTM Capital Dropbox\Eddy Winiarz\Trading\COF\Models\Unfinished Models\Eddy\Python Projects\work_supa\
```

### **GitHub Repository:**
```
https://github.com/ewswlw/work_supa.git
```

### **Key Branches:**
- **`master`**: Clean production version
- **`sql`**: Development version with raw data

---

## ⚡ **Quick Setup Commands**

### **GitHub Clone + Setup:**
```bash
# Clone repository
git clone https://github.com/ewswlw/work_supa.git
cd work_supa

# Switch to development branch (if needed)
git checkout sql

# Install dependencies
poetry install

# Run pipeline (if data needed)
poetry run python run_pipe.py --full
```

### **Dropbox Setup:**
```bash
# Navigate to project
cd "C:\Users\[Username]\YTM Capital Dropbox\Eddy Winiarz\Trading\COF\Models\Unfinished Models\Eddy\Python Projects\work_supa"

# Verify poetry environment
poetry env info

# Run pipeline (if updates needed)
poetry run python run_pipe.py --status
```

---

## 🔄 **Branch Information**

### **Current Branch Status:**
- **`sql` branch**: Your working version with all raw data
- **`master` branch**: Clean merged version for production
- **Remote sync**: Both branches properly synced

### **Which Branch to Use:**
- **Development work**: Use `sql` branch
- **Production/sharing**: Use `master` branch

---

## 💡 **Pro Tips**

1. **Check git status** first: `git status`
2. **Always use Poetry**: `poetry run python` instead of `python`
3. **Keep branches synced** when making changes
4. **Use .gitignore** to keep repository clean
5. **Log files are local only** - won't sync to repository

---

*Last Updated: 2025-01-27*
*Status: Both Dropbox and GitHub methods fully functional* 
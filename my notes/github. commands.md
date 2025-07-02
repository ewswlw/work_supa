# Git Commands to Reset Project to Match DB Branch

Based on the GitHub repository at https://github.com/ewswlw/work_supa/tree/db, here are the git commands to reset your current project to match exactly what's in the `db` branch:

## Option 1: Reset your current branch (master) to match the db branch exactly

```bash
# First, fetch the latest changes from remote
git fetch origin

# Reset your current branch to match the remote db branch exactly
# WARNING: This will overwrite any uncommitted changes
git reset --hard origin/db
```

## Option 2: If you want to preserve your current work first

```bash
# Save your current work to a backup branch (optional)
git branch backup-master

# Then reset to match db branch
git fetch origin
git reset --hard origin/db
```

## Option 3: Switch to db branch instead of resetting master

```bash
# Switch to the db branch
git checkout db

# Make sure it's up to date with remote
git pull origin db

# If you want to make this your new master branch
git checkout master
git reset --hard db
```

## What this will do:

Looking at your GitHub db branch, this will add these key files that are missing from your current master:
- `.cursorrules` - The comprehensive project rules file
- `.env` - Environment variables file  
- `.gitignore` - Git ignore patterns
- Any other files that exist in db but not in master

## ⚠️ Important Warning:

The `git reset --hard` command will **permanently delete** any uncommitted changes in your working directory. Make sure you've committed or backed up any important work before running this command.

## Recommended approach:

```bash
# 1. Check if you have any uncommitted changes
git status

# 2. If you have changes you want to keep, commit them first
git add .
git commit -m "Save current work before reset"

# 3. Create a backup branch (optional but recommended)
git branch backup-$(date +%Y%m%d)

# 4. Reset to match db branch
git fetch origin
git reset --hard origin/db

# 5. Verify the reset worked
git status
```

After running these commands, your project will look exactly like the [db branch on GitHub](https://github.com/ewswlw/work_supa/tree/db).

## Key Files That Will Be Added:

From the GitHub repository, these are the main files that will be added to your project:

### `.cursorrules`
Contains comprehensive project rules including:
- Project awareness & file management guidelines
- General coding standards
- Data validation & integrity rules
- Detailed logging & auditing requirements
- Performance & monitoring guidelines
- Database-specific patterns for trading systems

### `.env`
Environment variables file for the project

### `.gitignore`
Git ignore patterns including:
- Node.js installer files
- Python cache files
- VSCode settings
- System files
- MCP test folders

## Verification Steps:

After reset, verify these files exist:
```bash
ls -la | grep "^\."
# Should show: .cursorrules, .env, .gitignore
```

Check file contents:
```bash
head -10 .cursorrules
cat .gitignore
```

## Recovery Commands (if needed):

If you need to undo the reset:
```bash
# If you created a backup branch
git reset --hard backup-master

# Or use reflog to find previous state
git reflog
git reset --hard HEAD@{n}  # where n is the number from reflog
```

## Viewing GITs and RollBack

# View your recent commits
git log --oneline -5

# Rollback to the commit before the merge
git reset --soft 26d25a3

# Or revert the merge commit (safer)
git revert 231b386
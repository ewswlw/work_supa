### 1. Make Sure Your Local `master` is Up to Date

```sh
git checkout master
git pull origin master
```
*Ensures you're starting from the latest code.*
```

## Viewing GITs and RollBack

# View your recent commits
git log --oneline -5

# Rollback to the commit before the merge
git reset --soft 26d25a3

# Or revert the merge commit (safer)
git revert 231b386

## Push Your Current Project to the `master` Branch

If you are already on the `master` branch and have committed your changes:

```sh
git push origin master
```

---

### If you are on a different branch and want to merge your changes into `master` before pushing:

```sh
git checkout master
git pull origin master         # Update local master with remote changes
git merge your-feature-branch
git push origin master
```

*Replace `your-feature-branch` with the name of your branch.*

---
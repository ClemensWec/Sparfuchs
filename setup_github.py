"""
GitHub Setup Script für Sparfuchs.
Auth ist schon fertig — jetzt nur noch Repo erstellen und pushen.
"""
import subprocess
import sys

GH = r"C:\Program Files\GitHub CLI\gh.exe"

def run(args, check=True, **kwargs):
    print(f"\n> {' '.join(args)}")
    return subprocess.run(args, check=check, **kwargs)

# Step 1: Git Identity setzen
print("=" * 50)
print("SCHRITT 1: Git Identity setzen")
print("=" * 50)
run(["git", "config", "user.name", "ClemensWec"])
run(["git", "config", "user.email", "ClemensWec@users.noreply.github.com"])

# Step 2: Commit
print("\n" + "=" * 50)
print("SCHRITT 2: Commit erstellen")
print("=" * 50)
run(["git", "add", "-A"])
run(["git", "commit", "-m", "Initial commit: Sparfuchs grocery price comparison"])

# Step 3: Repo erstellen + push
print("\n" + "=" * 50)
print("SCHRITT 3: Repo erstellen und pushen")
print("=" * 50)
run([GH, "repo", "create", "sparfuchs", "--private", "--source=.", "--push"])

print("\n" + "=" * 50)
print("FERTIG! Dein Repo: https://github.com/ClemensWec/sparfuchs")
print("=" * 50)

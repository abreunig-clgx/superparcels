import os
import subprocess
import tomlkit
import shutil
import glob
import argparse
import datetime
from pathlib import Path

def bump_version(version: str, bump: str) -> str:
    major, minor, patch = map(int, version.split("."))
    if bump == "patch":
        patch += 1
    elif bump == "minor":
        minor += 1
        patch = 0
    elif bump == "major":
        major += 1
        minor = patch = 0
    else:
        raise ValueError("Bump must be 'patch', 'minor', or 'major'")
    return f"{major}.{minor}.{patch}"

def update_pyproject_version(bump_type: str, dry_run=False):
    with open("pyproject.toml", "r", encoding="utf-8") as f:
        data = tomlkit.parse(f.read())

    current_version = data["project"]["version"]
    new_version = bump_version(current_version, bump_type)

    if not dry_run:
        data["project"]["version"] = new_version
        with open("pyproject.toml", "w", encoding="utf-8") as f:
            f.write(tomlkit.dumps(data))

    return current_version, new_version

def run_command(cmd, dry_run=False):
    print(f"→ {cmd}")
    if not dry_run:
        subprocess.run(cmd, shell=True, check=True)

def check_git_clean():
    result = subprocess.run("git status --porcelain", shell=True, capture_output=True, text=True)
    if result.stdout.strip():
        print("❌ Git working directory is not clean. Commit or stash changes before releasing.")
        exit(1)

def generate_changelog(version, previous_version):
    timestamp = datetime.date.today().isoformat()
    changelog_path = Path("CHANGELOG.md")

    try:
        log = subprocess.check_output(
            f'git log v{previous_version}..HEAD --pretty=format:"- %s"',
            shell=True,
            text=True
        )
        entry = f"## v{version} - {timestamp}\n\n{log.strip()}\n\n"
    except subprocess.CalledProcessError:
        entry = f"## v{version} - {timestamp}\n\n- _No previous tag found; update manually._\n\n"

    if changelog_path.exists():
        content = changelog_path.read_text()
        changelog_path.write_text(entry + content)
    else:
        changelog_path.write_text("# Changelog\n\n" + entry)

    print("📝 CHANGELOG.md updated")
    return entry.strip()



def clean_build_dirs():
    for path in ["dist", "build"]:
        shutil.rmtree(path, ignore_errors=True)

    for egg_info in glob.glob("*.egg-info"):
        shutil.rmtree(egg_info, ignore_errors=True)



def main():
    parser = argparse.ArgumentParser(description="Automate release flow")
    parser.add_argument("bump", choices=["patch", "minor", "major"], help="Type of version bump")
    parser.add_argument("--dry-run", action="store_true", help="Simulate everything without making changes")
    parser.add_argument("--prerelease", action="store_true", help="Mark the GitHub release as a pre-release")

    args = parser.parse_args()

    # Prevent releasing from any branch other than main
    branch = subprocess.check_output("git rev-parse --abbrev-ref HEAD", shell=True).decode().strip()
    if branch != "master" and not args.dry_run:
        print(f"❌ You are on '{branch}', not 'main'. Release only from main!")
        exit(1)

    if not args.dry_run:
        check_git_clean()

    prev_version, new_version = update_pyproject_version(args.bump, dry_run=args.dry_run)

    print(f"\n🔧 Version bump: {prev_version} → {new_version}\n")

    if not args.dry_run:
        clean_build_dirs()
    else:
        print("🧼 [dry-run] Would clean: dist/, build/, and *.egg-info")

    run_command("python -m build", dry_run=args.dry_run)

    # Step 1: bump version
    run_command("git add pyproject.toml", dry_run=args.dry_run)
    run_command(f'git commit -m "Bump version to {new_version}"', dry_run=args.dry_run)
    run_command(f"git tag v{new_version}", dry_run=args.dry_run)

    # Step 2: generate changelog
    release_notes = generate_changelog(new_version, prev_version)
    run_command("git add CHANGELOG.md", dry_run=args.dry_run)
    run_command(f'git commit -m "Update changelog for v{new_version}"', dry_run=args.dry_run)

    # Step 3: push
    run_command("git push origin main --tags", dry_run=args.dry_run)

    wheel_path = f"dist/sp-{new_version}-py3-none-any.whl"

    cmd = f'gh release create v{new_version} {wheel_path} -t "v{new_version}"'
    if release_notes:
        cmd += f' -n "{release_notes}"'
    
    if args.prerelease:
        cmd += " --prerelease"

    run_command(cmd, dry_run=args.dry_run)

    print(f"\n✅ Release v{new_version} complete! 🎉")

if __name__ == "__main__":
    main()

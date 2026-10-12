import subprocess
import sys

REPO = "https://github.com/SusmithaDuddukuri/Github_ML"
SCRIPT_NAME = "create_next_10_prs.py"

TASKS = [
    (
        "9-category-too-short",
        "test(api): reject category that is too short",
        "test_category_too_short",
        '''    with pytest.raises(ValidationError):
        AlertCreateInput(
            title="Flood warning",
            category="X",
            severity="High",
            region="North",
            latitude=10,
            longitude=20,
        )'''
    ),
    (
        "10-invalid-severity",
        "test(api): reject invalid severity",
        "test_invalid_alert_severity",
        '''    with pytest.raises(ValidationError):
        AlertCreateInput(
            title="Flood warning",
            category="Flood",
            severity="Urgent",
            region="North",
            latitude=10,
            longitude=20,
        )'''
    ),
    (
        "11-invalid-latitude",
        "test(api): reject latitude outside valid range",
        "test_latitude_outside_valid_range",
        '''    with pytest.raises(ValidationError):
        AlertCreateInput(
            title="Flood warning",
            category="Flood",
            severity="High",
            region="North",
            latitude=91,
            longitude=20,
        )'''
    ),
    (
        "12-invalid-longitude",
        "test(api): reject longitude outside valid range",
        "test_longitude_outside_valid_range",
        '''    with pytest.raises(ValidationError):
        AlertCreateInput(
            title="Flood warning",
            category="Flood",
            severity="High",
            region="North",
            latitude=10,
            longitude=181,
        )'''
    ),
    (
        "13-confidence-below-minimum",
        "test(api): reject confidence below minimum",
        "test_confidence_below_minimum",
        '''    with pytest.raises(ValidationError):
        AlertCreateInput(
            title="Flood warning",
            category="Flood",
            severity="High",
            region="North",
            latitude=10,
            longitude=20,
            confidence=0,
        )'''
    ),
    (
        "14-confidence-above-maximum",
        "test(api): reject confidence above maximum",
        "test_confidence_above_maximum",
        '''    with pytest.raises(ValidationError):
        AlertCreateInput(
            title="Flood warning",
            category="Flood",
            severity="High",
            region="North",
            latitude=10,
            longitude=20,
            confidence=101,
        )'''
    ),
    (
        "15-invalid-review-status",
        "test(api): reject invalid review status",
        "test_invalid_review_status",
        '''    with pytest.raises(ValidationError):
        AlertReviewInput(status="Pending")'''
    ),
    (
        "16-review-notes-too-long",
        "test(api): reject review notes exceeding limit",
        "test_review_notes_exceeding_maximum_length",
        '''    with pytest.raises(ValidationError):
        AlertReviewInput(
            status="Verified",
            review_notes="x" * 1001,
        )'''
    ),
    (
        "17-description-too-long",
        "test(api): reject descriptions exceeding limit",
        "test_alert_description_exceeding_maximum_length",
        '''    with pytest.raises(ValidationError):
        AlertCreateInput(
            title="Flood warning",
            category="Flood",
            severity="High",
            region="North",
            latitude=10,
            longitude=20,
            description="x" * 2001,
        )'''
    ),
    (
        "18-short-full-name",
        "test(api): reject registration name that is too short",
        "test_registration_name_too_short",
        '''    with pytest.raises(ValidationError):
        RegisterInput(
            email="person@example.com",
            full_name="A",
            password="long-enough-password",
        )'''
    ),
]


def run(args, capture=False, check=True):
    result = subprocess.run(
        args,
        text=True,
        capture_output=capture,
    )
    if check and result.returncode != 0:
        if capture:
            print(result.stdout)
            print(result.stderr, file=sys.stderr)
        raise RuntimeError(
            f"Command failed ({result.returncode}): {' '.join(args)}"
        )
    return result


def main():
    if run(["gh", "auth", "status"], check=False).returncode != 0:
        print("Sign in to GitHub first: gh auth login")
        return

    state = run(
        ["gh", "pr", "view", "8", "--repo", REPO,
         "--json", "state", "--jq", ".state"],
        capture=True,
    ).stdout.strip()

    if state != "MERGED":
        print(f"PR #8 is currently {state}. Merge it before running this batch.")
        return

    run(["git", "fetch", "origin"])

    status = run(
        ["git", "status", "--porcelain"],
        capture=True,
    ).stdout.splitlines()

    unexpected = [
        line for line in status
        if not line[3:].replace("/", "\\").endswith(
            ("create_10_prs.py", "create_next_10_prs.py")
        )
    ]
    if unexpected:
        print("Stop: other working-tree changes were found:")
        print("\n".join(unexpected))
        print("Save or commit those changes before continuing.")
        return

    run(["git", "switch", "main"])
    run(["git", "pull", "--ff-only", "origin", "main"])

    created = []

    for branch_suffix, title, test_name, assertion in TASKS:
        branch = f"test/{branch_suffix}"

        local_branch = run(
            ["git", "show-ref", "--verify", "--quiet",
             f"refs/heads/{branch}"],
            check=False,
        ).returncode == 0

        remote_branch = run(
            ["git", "ls-remote", "--exit-code", "--heads",
             "origin", branch],
            capture=True,
            check=False,
        ).returncode == 0

        if local_branch or remote_branch:
            print(f"STOP: branch {branch} already exists.")
            print("Check GitHub for its existing PR before continuing.")
            break

        run(["git", "switch", "main"])
        run(["git", "pull", "--ff-only", "origin", "main"])
        run(["git", "switch", "-c", branch])

        test_code = f'''

def {test_name}():
    import pytest
    from pydantic import ValidationError
    from app.schemas import (
        AlertCreateInput,
        AlertReviewInput,
        RegisterInput,
    )

{assertion}
'''

        with open("tests/test_api.py", "a", encoding="utf-8") as file:
            file.write(test_code)

        print(f"\\nRunning tests for {branch}...")
        result = run(
            [sys.executable, "-m", "pytest", "tests", "-q"],
            check=False,
        )

        if result.returncode != 0:
            print("Tests failed. No PR will be created for this branch.")
            run(["git", "switch", "main"])
            break

        run(["git", "add", "tests/test_api.py"])
        run(["git", "commit", "-m", title])
        run(["git", "push", "-u", "origin", branch])

        url = run(
            [
                "gh", "pr", "create",
                "--repo", REPO,
                "--base", "main",
                "--head", branch,
                "--title", title,
                "--body",
                f"Adds a focused schema-validation regression test "
                f"for `{test_name}`. Verified locally with "
                f"`python -m pytest tests -q`."
            ],
            capture=True,
        ).stdout.strip()

        created.append((branch, url))
        print(f"PR created: {url}")

        run(["git", "switch", "main"])

    print("\\n========== BATCH RESULTS ==========")
    print(f"PRs created in this run: {len(created)}")
    for branch, url in created:
        print(f"{branch}: {url}")

    print("\\nAll created PRs are left open for review.")
    print("No PRs were merged automatically.")


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, OSError) as error:
        print(f"\\nStopped safely: {error}")
        print("Check GitHub and git status before rerunning.")
        sys.exit(1)
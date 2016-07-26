#! /usr/bin/env python3

import click
import os
from subprocess import CalledProcessError, check_call, check_output
import sys

@click.command()
@click.argument('html_path')
def main(html_path):
    """Takes HTML_PATH and replaces the contents of the gh-pages branch with
    the contents of that directory (with a .nojekyll file).

    Will automatically create and manage the gh-pages branch (destructive if
    any hand-crafted work was in there) WITHOUT constantly deleting local
    source files to change to the gh-pages branch.

    Thanks to https://gist.github.com/cobyism/4730490 for the idea.
    """
    if not os.path.isdir(html_path):
        raise ValueError("Bad HTML path: not a directory")
    if not os.path.lexists(os.path.join(html_path, 'index.html')):
        raise ValueError("Bad HTML path: no index.html file")

    # Ensure in git repo
    gitBase = os.path.abspath(os.getcwd())
    while not os.path.lexists(os.path.join(gitBase, '.git')):
        old = gitBase
        gitBase = os.path.dirname(gitBase)
        if gitBase == old:
            sys.stderr.write("This does not appear to be within a git "
                    "repository; this tool is for Github.\n")
            sys.exit(1)

    nojek = os.path.join(html_path, '.nojekyll')
    if not os.path.lexists(nojek):
        # Add a .nojekyll file
        with open(nojek, 'w') as f:
            pass

    # Ensure that there is nothing added to the index, ready to be committed,
    # as this script might trigger a commit.
    try:
        check_call([ 'git', 'diff-index', '--quiet', '--cached', 'HEAD' ])
    except CalledProcessError:
        sys.stderr.write("*"*79 + "\n")
        sys.stderr.write("gh-pages can only be made when there is nothing "
                "currently 'git add'ed to the index; as gh-pages might "
                "trigger a commit, anything in the index would be committed "
                "under an irrelevant message.\n")
        sys.exit(1)

    if check_output([ 'git', 'status', '--porcelain', html_path ]).strip():
        # Commit the built documentation
        check_call([ 'git', 'add', '-fA', html_path ])
        check_call([ 'git', 'commit', '-m',
                'Updating documentation for gh-pages' ])

    print("Please open in browser: file://{}".format(os.path.abspath(
            os.path.join(html_path, 'index.html'))))
    print("If everything looks OK, entering [Y] will update the gh-pages "
            "branch at origin.")
    while True:
        r = input("Proceed? [Y/n] ")
        if r.lower() == 'n':
            print("Aborting.")
            sys.exit(1)
        elif r == 'Y':
            print("Continuing.")
            break

    subtree = os.path.relpath(os.path.abspath(html_path), gitBase)
    old = os.getcwd()
    try:
        os.chdir(gitBase)
        ghpages = check_output([ 'git', 'subtree', 'push', '--prefix',
                subtree, 'origin', 'gh-pages' ]).strip().decode('utf-8')
    finally:
        os.chdir(old)
    print("Done.")


if __name__ == '__main__':
    main()


#! /usr/bin/env python3

import click
import os
from subprocess import CalledProcessError, check_call, check_output

def _checkBranch():
    try:
        check_call([ 'git', 'rev-parse', '--verify', 'gh-pages' ])
    except CalledProcessError:
        # Need to make the branch


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

    # Add a .nojekyll file
    with open(os.path.join(html_path, '.nojekyll'), 'w') as f:
        pass

    print("HTML Path: {}".format(html_path))

if __name__ == '__main__':
    main()


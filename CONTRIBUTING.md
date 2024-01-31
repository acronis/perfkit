# Contributing

We're dedicated to enhancing perfkit to be the premier performance toolkit, 
and we'd greatly appreciate your contribution!

Should you wish to suggest new exported APIs, we kindly ask that you initiate
the process by [creating an issue][open-issue] to outline your proposal.
Engaging in discussions about API modifications before submitting pull requests
significantly streamlines the review process. Throughout your interactions,
including in issues, pull requests, and any other forms of communication,
it's crucial to maintain a respectful attitude towards all contributors.
We are committed to our [code of conduct](CODE_OF_CONDUCT.md) and expect all members of our community
to uphold these standards.

## Setup

[Fork][fork], then clone the repository:

```bash
mkdir -p $GOPATH/src/github.com/acronis
cd $GOPATH/src/github.com/acronis
git clone git@github.com:your_github_username/perfkit.git
cd perfkit
git remote add upstream https://github.com/acronis/perfkit.git
git fetch upstream
```

## Making Changes

Start by creating a new branch for your changes:

```bash
cd $GOPATH/src/github.com/acronis/perfkit
git checkout master
git fetch upstream
git rebase upstream/master
git checkout -b cool_new_feature
```

Make your changes, then ensure that `make lint` and `make test` still pass. 
If you're satisfied with your changes, push them to your fork.

```bash
git push origin cool_new_feature
```

Next, proceed to initiate a pull request using the GitHub interface.

From here, the ball is in our court to examine your proposed modifications.
We aim to address issues and pull requests promptly, typically within a few
business days, and we might propose enhancements or different approaches during
the review process. After your changes receive approval, a project maintainer
will integrate them into the project.

To increase the likelihood of your changes being accepted, it's beneficial if you:

Incorporate tests for any new features.
Craft a [good commit message][commit-message].
Ensure that your changes are backward-compatible.

[fork]: https://github.com/acronis/acronis-perflib/fork

[open-issue]: https://github.com/acronis/acronis-perflib/issues/new

[commit-message]: http://tbaggery.com/2008/04/19/a-note-about-git-commit-messages.html